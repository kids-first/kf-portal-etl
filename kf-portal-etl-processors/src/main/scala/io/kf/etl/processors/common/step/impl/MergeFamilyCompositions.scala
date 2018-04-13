package io.kf.etl.processors.common.step.impl

import io.kf.etl.dbschema.TFamilyRelationship
import io.kf.etl.model.{Family, FamilyComposition, FamilyMember, Participant}
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ListBuffer

class MergeFamilyCompositions(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]] {
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
    import ctx.spark.implicits._

    /**
      * flattenedFamilyRelationship is a map
      * key  : participant id
      * value: list of relationship which the current participant holds in the whole family
      */
    val flattenedFamilyRelationship =
      ctx.spark.sparkContext.broadcast(
        ctx.dbTables.familyRelationship.flatMap(tf => {
          Seq(
            tf,
            TFamilyRelationship(
              kfId = tf.kfId,
              uuid = tf.uuid,
              createdAt = tf.createdAt,
              modifiedAt = tf.modifiedAt,
              participantId = tf.relativeId,
              relativeId = tf.participantId,
              relativeToParticipantRelation = tf.participantToRelativeRelation,
              participantToRelativeRelation = tf.relativeToParticipantRelation
            )
          )
        }).groupByKey(tf => tf.participantId)
          .mapGroups((participant_id, iterator) => {
            (
              participant_id,
              iterator.collect{
                case tf: TFamilyRelationship if(tf.participantToRelativeRelation.isDefined) => {
                  tf.participantToRelativeRelation.get
                }
              }.toSeq
            )
          })
          .collect().map(tuple => (tuple._1, tuple._2.toSet))
          .toMap
      )

    participants.groupByKey(participant => {
      participant.family match {
        case Some(family) => Some(family.familyId)
        case None => None
      }
    }).flatMapGroups((family_id, iterator) => {
      FamilyCompositionTypeDeducer.deduceFamilyCompositions(family_id, iterator.toSeq, flattenedFamilyRelationship)
    })
  }
}

object FamilyCompositionTypeDeducer {
  case class FamilyStructure(father: Option[Participant] = None, mother: Option[Participant] = None, probandChild: Option[Participant] = None, otherChildren: Seq[Participant] = Seq.empty)
  class ProbandMissingInFamilyException extends Exception("Family has no proband child!")
  /*
    familyRelationship map:
    key  : participant id
    value: list of relationship in the family, for example, father, mother, child
   */
  def deduceFamilyCompositions(familyId:Option[String], family: Seq[Participant], familyRelationship_broadcast: Broadcast[Map[String, Set[String]]]): Seq[Participant] = {

    val familyRelationship = familyRelationship_broadcast.value
    familyId match {
      case None => family
      case Some(id) => {
        // iterate the family members to fill in FamilyStructure instance
        // then based on how the family roles are filled to determine composition values

        val familyStructure =
          family.foldLeft(FamilyStructure()){(family_structure, participant) => {
            familyRelationship.get(participant.kfId) match {
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

        val family_availableDataTypes = getAvailableDataTypes(family)
        val family_sharedHpoIds = getSharedHpoIds(family)

        val father_compositions = new ListBuffer[FamilyComposition]
        val mother_compositions = new ListBuffer[FamilyComposition]

        val proband_compositions = new scala.collection.mutable.HashMap[String, FamilyComposition]

        familyStructure match {
          case FamilyStructure(None, None, None, List()) => {
            Seq.empty
          }
          case FamilyStructure(_, _, None, _) => {
            family
          }
          case FamilyStructure(Some(father), Some(mother), Some(proband), _) => {
            // complete_trio
            val sharedHpoIds = getSharedHpoIds(Seq(father, mother, proband))
            val availableDataTypes = getAvailableDataTypes(Seq(father, mother, proband))
            val trio =
              FamilyComposition(
                composition = "complete_trio",
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", sharedHpoIds),
                  getFamilyMemberFromParticipant(mother, "mother", sharedHpoIds),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds)
                )
              )
            val other =
              FamilyComposition(
                composition = "other",
                sharedHpoIds = family_sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", family_sharedHpoIds),
                  getFamilyMemberFromParticipant(mother, "mother", family_sharedHpoIds),
                  getFamilyMemberFromParticipant(proband, "child", family_sharedHpoIds)
                ) ++ familyStructure.otherChildren.map(child => {
                  getFamilyMemberFromParticipant(child, "child", family_sharedHpoIds)
                })
              )

            Seq(
              father.copy(family = Some(
                Family(
                  familyId = father.family.get.familyId,
                  familyCompositions = Seq(trio, other)
                )
              )),
              mother.copy(family = Some(
                Family(
                  familyId = mother.family.get.familyId,
                  familyCompositions = Seq(trio, other)
                )
              )),
              proband.copy(family = Some(
                Family(
                  familyId = proband.family.get.familyId,
                  familyCompositions = Seq(trio, other)
                )
              ))
            ) ++ familyStructure.otherChildren.map(child => {
              child.copy(family = Some(
                Family(
                  familyId = child.family.get.familyId,
                  familyCompositions = Seq(other)
                )
              ))
            })
          } // end of FamilyStructure(Some(father), Some(mother), Some(proband), _)
          case FamilyStructure(Some(father), None, Some(proband), _) => {
            // partial_trio
            val sharedHpoIds = getSharedHpoIds(Seq(father, proband))
            val availableDataTypes = getAvailableDataTypes(Seq(father, proband))
            val trio =
              FamilyComposition(
                composition = "complete_trio",
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", sharedHpoIds),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds)
                )
              )
            val other =
              FamilyComposition(
                composition = "other",
                sharedHpoIds = family_sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", family_sharedHpoIds),
                  getFamilyMemberFromParticipant(proband, "child", family_sharedHpoIds)
                ) ++ familyStructure.otherChildren.map(child => {
                  getFamilyMemberFromParticipant(child, "child", family_sharedHpoIds)
                })
              )

            Seq(
              father.copy(family = Some(
                Family(
                  familyId = father.family.get.familyId,
                  familyCompositions = Seq(trio, other)
                )
              )),
              proband.copy(family = Some(
                Family(
                  familyId = proband.family.get.familyId,
                  familyCompositions = Seq(trio, other)
                )
              ))
            ) ++ familyStructure.otherChildren.map(child => {
              child.copy(family = Some(
                Family(
                  familyId = child.family.get.familyId,
                  familyCompositions = Seq(other)
                )
              ))
            })
          } // end of FamilyStructure(Some(father), None, Some(proband), _)
          case FamilyStructure(None, Some(mother), Some(proband), _) => {
            // partial_trio
            val sharedHpoIds = getSharedHpoIds(Seq(mother, proband))
            val availableDataTypes = getAvailableDataTypes(Seq(mother, proband))
            val trio =
              FamilyComposition(
                composition = "complete_trio",
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(mother, "mother", sharedHpoIds),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds)
                )
              )
            val other =
              FamilyComposition(
                composition = "other",
                sharedHpoIds = family_sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(mother, "mother", family_sharedHpoIds),
                  getFamilyMemberFromParticipant(proband, "child", family_sharedHpoIds)
                ) ++ familyStructure.otherChildren.map(child => {
                  getFamilyMemberFromParticipant(child, "child", family_sharedHpoIds)
                })
              )

            Seq(
              mother.copy(family = Some(
                Family(
                  familyId = mother.family.get.familyId,
                  familyCompositions = Seq(trio, other)
                )
              )),
              proband.copy(family = Some(
                Family(
                  familyId = proband.family.get.familyId,
                  familyCompositions = Seq(trio, other)
                )
              ))
            ) ++ familyStructure.otherChildren.map(child => {
              child.copy(family = Some(
                Family(
                  familyId = child.family.get.familyId,
                  familyCompositions = Seq(other)
                )
              ))
            })
          } // end of case FamilyStructure(None, Some(mother), Some(proband), _)
        }// end familyStructure match

      }
    }
  }

  def getFamilyMemberFromParticipant(participant: Participant, relationship:String, familySharedHpoIds:Seq[String]): FamilyMember = {
    FamilyMember(
      kfId = participant.kfId,
      uuid = participant.uuid,
      createdAt = participant.createdAt,
      modifiedAt = participant.modifiedAt,
      isProband = participant.isProband,
      availableDataTypes = participant.availableDataTypes,
      phenotype = participant.phenotype.map(pt => {
        pt.copy(hpo = Some(
          pt.hpo.get.copy(sharedHpoIds = familySharedHpoIds)
        ))
      }),
      study = participant.study,
      race = participant.race,
      ethnicity = participant.ethnicity,
      relationship = Some(relationship)
    )
  }

  def getAvailableDataTypes(participants: Seq[Participant]): Seq[String] = {
    participants.tail.foldLeft(participants.head.availableDataTypes){(types, participant) => {
      types.intersect(participant.availableDataTypes)
    }}
  }

  def getSharedHpoIds(participants: Seq[Participant]): Seq[String] = {
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