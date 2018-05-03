package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{FamilyComposition_ES, FamilyMember_ES, Family_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EFamilyRelationship
import io.kf.etl.model.utils.{ParticipantId_AvailableDataTypes, ParticipantId_BiospecimenId}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ListBuffer

class MergeFamily(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {

    import ctx.spark.implicits._
    val availableDataTypes_broadcast = calculcateAvailableDataTypes(ctx.entityDataset)

    /**
      * flattenedFamilyRelationship is a map
      * key  : participant id
      * value: list of relationship which the current participant holds in the whole family
      */
    val flattenedFamilyRelationship =
      ctx.spark.sparkContext.broadcast(
        ctx.entityDataset.familyRelationships.flatMap(tf => {
          Seq(
            tf,
            EFamilyRelationship(
              kfId = tf.kfId,
              createdAt = tf.createdAt,
              modifiedAt = tf.modifiedAt,
              participantId = tf.relativeId,
              relativeId = tf.participantId,
              relativeToParticipantRelation = tf.participantToRelativeRelation,
              participantToRelativeRelation = tf.relativeToParticipantRelation
            )
          )
        }).groupByKey(tf => tf.participantId.get)
          .mapGroups((participant_id, iterator) => {
            (
              participant_id,
              iterator.collect{
                case tf: EFamilyRelationship if(tf.participantToRelativeRelation.isDefined) => {
                  tf.participantToRelativeRelation.get
                }
              }.toSeq
            )
          })
          .collect().groupBy(_._1).map(tuple => {
            (
              tuple._1,
              tuple._2.flatMap(_._2)
            )
          })
          .map(tuple => (tuple._1, tuple._2.toSet))
      )

    participants.groupByKey(_.familyId).flatMapGroups((family_id, iterator) => {
      MergeFamily.deduceFamilyCompositions(family_id, iterator.toSeq, flattenedFamilyRelationship, availableDataTypes_broadcast)
    })

  }

  private def calculcateAvailableDataTypes(data: EntityDataSet): Broadcast[Map[String, Seq[String]]] = {
    import ctx.spark.implicits._

    val par_bio =
      data.participants.joinWith(data.biospecimens, data.participants.col("kfId") === data.biospecimens.col("participantId")).map(tuple => {
        ParticipantId_BiospecimenId(parId = tuple._1.kfId.get, bioId = tuple._2.kfId.get)
      })

    ctx.spark.sparkContext.broadcast[Map[String, Seq[String]]](
      par_bio.joinWith(data.genomicFiles, par_bio.col("bioId") ===  data.genomicFiles.col("biospecimenId")).groupByKey(tuple => {
        tuple._1.parId
      }).mapGroups((parId, iterator) => {
        val seq = iterator.toSeq
        ParticipantId_AvailableDataTypes(
          parId,
          iterator.map(_._2.dataType).collect{
            case Some(datatype) => datatype
          }.toSeq
        )
      }).collect().map(item => {
        (item.parId, item.availableDataTypes)
      }).toMap
    )
  }
}

object MergeFamily {
  case class FamilyStructure(father: Option[Participant_ES] = None, mother: Option[Participant_ES] = None, probandChild: Option[Participant_ES] = None, otherChildren: Seq[Participant_ES] = Seq.empty)
  class ProbandMissingInFamilyException extends Exception("Family has no proband child!")
  /*
    familyRelationship map:
    key  : participant id
    value: list of relationship in the family, for example, father, mother, child
   */
  def deduceFamilyCompositions(familyId:Option[String], family: Seq[Participant_ES], familyRelationship_broadcast: Broadcast[Map[String, Set[String]]], availableDataTypes_broadcast: Broadcast[Map[String, Seq[String]]]): Seq[Participant_ES] = {

    val familyRelationship = familyRelationship_broadcast.value
    val mapOfAvailableDataTypes = availableDataTypes_broadcast.value
    familyId match {
      case None => family
      case Some(id) => {
        // iterate the family members to fill in FamilyStructure instance
        // then based on how the family roles are filled to determine composition values

        val familyStructure =
          family.foldLeft(FamilyStructure()){(family_structure, participant) => {
            familyRelationship.get(participant.kfId.get) match {
              case None => family_structure
              case Some(relationships) => {
                if(relationships.contains("father"))
                  family_structure.copy(father = Some(participant))
                else if(relationships.contains("mother"))
                  family_structure.copy(mother = Some(participant))
                else if(relationships.contains("child")) {
                  if (participant.isProband.isDefined && participant.isProband.get)
                    family_structure.copy(probandChild =Some(participant))
                  else
                    family_structure.copy(otherChildren = (family_structure.otherChildren :+ participant))
                }
                else // not check other relationship values such as "uncle", "aunt" etc
                  family_structure

              }
            }
          }}

        val family_availableDataTypes = getAvailableDataTypes(family, mapOfAvailableDataTypes)
        val family_sharedHpoIds = getSharedHpoIds(family)

        val father_compositions = new ListBuffer[FamilyComposition_ES]
        val mother_compositions = new ListBuffer[FamilyComposition_ES]

        val proband_compositions = new scala.collection.mutable.HashMap[String, FamilyComposition_ES]

        familyStructure match {
          case FamilyStructure(Some(father), Some(mother), Some(proband), _) => {
            // complete_trio
            val sharedHpoIds = getSharedHpoIds(Seq(father, mother, proband))
            val availableDataTypes = getAvailableDataTypes(Seq(father, mother, proband), mapOfAvailableDataTypes)
            val trio =
              FamilyComposition_ES(
                composition = Some("complete_trio"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(mother, "mother", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                )
              )
            val other =
              FamilyComposition_ES(
                composition = Some("other"),
                sharedHpoIds = family_sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", family_sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(mother, "mother", family_sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", family_sharedHpoIds, mapOfAvailableDataTypes)
                ) ++ familyStructure.otherChildren.map(child => {
                  getFamilyMemberFromParticipant(child, "child", family_sharedHpoIds, mapOfAvailableDataTypes)
                })
              )

            Seq(
              father.copy(family = Some(
                Family_ES(
                  familyId = father.familyId.get,
                  familyCompositions = Seq(trio, other),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              )),
              mother.copy(family = Some(
                Family_ES(
                  familyId = mother.familyId.get,
                  familyCompositions = Seq(trio, other),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              )),
              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(trio, other),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ))
            ) ++ familyStructure.otherChildren.map(child => {
              child.copy(family = Some(
                Family_ES(
                  familyId = child.familyId.get,
                  familyCompositions = Seq(other),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ))
            })
          } // end of FamilyStructure(Some(father), Some(mother), Some(proband), _)
          case FamilyStructure(Some(father), None, Some(proband), _) => {
            // partial_trio
            val sharedHpoIds = getSharedHpoIds(Seq(father, proband))
            val availableDataTypes = getAvailableDataTypes(Seq(father, proband), mapOfAvailableDataTypes)
            val trio =
              FamilyComposition_ES(
                composition = Some("complete_trio"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                )
              )
            val other =
              FamilyComposition_ES(
                composition = Some("other"),
                sharedHpoIds = family_sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", family_sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", family_sharedHpoIds, mapOfAvailableDataTypes)
                ) ++ familyStructure.otherChildren.map(child => {
                  getFamilyMemberFromParticipant(child, "child", family_sharedHpoIds, mapOfAvailableDataTypes)
                })
              )

            Seq(
              father.copy(family = Some(
                Family_ES(
                  familyId = father.familyId.get,
                  familyCompositions = Seq(trio, other),
                  fatherId = father.kfId
                )
              )),
              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(trio, other),
                  fatherId = father.kfId
                )
              ))
            ) ++ familyStructure.otherChildren.map(child => {
              child.copy(family = Some(
                Family_ES(
                  familyId = child.familyId.get,
                  familyCompositions = Seq(other),
                  fatherId = father.kfId
                )
              ))
            })
          } // end of FamilyStructure(Some(father), None, Some(proband), _)
          case FamilyStructure(None, Some(mother), Some(proband), _) => {
            // partial_trio
            val sharedHpoIds = getSharedHpoIds(Seq(mother, proband))
            val availableDataTypes = getAvailableDataTypes(Seq(mother, proband), mapOfAvailableDataTypes)
            val trio =
              FamilyComposition_ES(
                composition = Some("complete_trio"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(mother, "mother", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                )
              )
            val other =
              FamilyComposition_ES(
                composition = Some("other"),
                sharedHpoIds = family_sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(mother, "mother", family_sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", family_sharedHpoIds, mapOfAvailableDataTypes)
                ) ++ familyStructure.otherChildren.map(child => {
                  getFamilyMemberFromParticipant(child, "child", family_sharedHpoIds, mapOfAvailableDataTypes)
                })
              )

            Seq(
              mother.copy(family = Some(
                Family_ES(
                  familyId = mother.familyId.get,
                  familyCompositions = Seq(trio, other),
                  motherId = mother.kfId
                )
              )),
              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(trio, other),
                  motherId = mother.kfId
                )
              ))
            ) ++ familyStructure.otherChildren.map(child => {
              child.copy(family = Some(
                Family_ES(
                  familyId = child.familyId.get,
                  familyCompositions = Seq(other),
                  motherId = mother.kfId
                )
              ))
            })
          } // end of case FamilyStructure(None, Some(mother), Some(proband), _)
          case _ => family
        }// end familyStructure match

      }
    }
  }

  def getFamilyMemberFromParticipant(participant: Participant_ES, relationship:String, familySharedHpoIds:Seq[String], mapOfAvailableDataTypes: Map[String, Seq[String]]): FamilyMember_ES = {
    FamilyMember_ES(
      kfId = participant.kfId,
      createdAt = participant.createdAt,
      modifiedAt = participant.modifiedAt,
      isProband = participant.isProband,
      availableDataTypes = mapOfAvailableDataTypes.get(participant.kfId.get) match {
        case Some(types) => types
        case None => Seq.empty
      },
      phenotype = participant.phenotype,
      study = participant.study,
      race = participant.race,
      ethnicity = participant.ethnicity,
      relationship = Some(relationship)
    )
  }

  def getAvailableDataTypes(participants: Seq[Participant_ES], mapOfAvailableDataTypes: Map[String, Seq[String]]): Seq[String] = {

    val seqOfDataTypes =
      participants.map(participant => {
        mapOfAvailableDataTypes.get(participant.kfId.get) match {
          case Some(seq) => seq
          case None => Seq.empty
        }
      })

    seqOfDataTypes.tail.foldLeft(seqOfDataTypes.head){(left, right) => {
      left.intersect(right)
    }}
  }

  def getSharedHpoIds(participants: Seq[Participant_ES]): Seq[String] = {
    participants.tail.foldLeft(
      participants.head.phenotype.flatMap(pt => {
        pt.hpo match {
          case None => None
          case Some(hpo) => {
            Some(hpo.hpoIds)
          }
        }
      }).toList.flatten
    ){(hpos, participant) => {
      participant.phenotype.flatMap(pt => {
        pt.hpo match {
          case None => None
          case (Some(hpo)) => {
            Some(hpo.hpoIds)
          }
        }
      }).toList.flatten
    }}
  }

}