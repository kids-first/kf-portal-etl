package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{FamilyComposition_ES, FamilyMember_ES, Family_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EFamilyRelationship
import io.kf.etl.model.utils.{ParticipantId_AvailableDataTypes, ParticipantId_BiospecimenId}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset

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
        ctx.entityDataset.familyRelationships.map(fr => {
          fr.copy(
            participant1ToParticipant2Relation = Some(fr.participant2ToParticipant1Relation.get.toLowerCase),
            participant2ToParticipant1Relation = Some(fr.participant1ToParticipant2Relation.get.toLowerCase)
          )
        }).flatMap(tf => {
          Seq(
            tf,
            EFamilyRelationship(
              kfId = tf.kfId,
              createdAt = tf.createdAt,
              modifiedAt = tf.modifiedAt,
              participant1 = tf.participant2,
              participant2 = tf.participant1,
              participant2ToParticipant1Relation = tf.participant1ToParticipant2Relation,
              participant1ToParticipant2Relation = tf.participant2ToParticipant1Relation
            )
          )
        }).groupByKey(tf => tf.participant1.get)
          .mapGroups((participant_id, iterator) => {
            (
              participant_id,
              iterator.collect{
                case tf: EFamilyRelationship if(tf.participant1ToParticipant2Relation.isDefined) => {
                  tf.participant1ToParticipant2Relation.get
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
  case class FamilyStructure(father: Option[Participant_ES] = None, mother: Option[Participant_ES] = None, proband: Option[Participant_ES] = None, others: Seq[(String, Participant_ES)] = Seq.empty)
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
        val familyStructure =
          family.foldLeft(FamilyStructure()){(family_structure, participant) => {
            familyRelationship.get(participant.kfId.get) match {
              case None => {
                participant.isProband match {
                  case Some(isProband) if isProband == true => {
                    family_structure.copy(proband = Some(participant))
                  }
                  case _ => {
                    family_structure.copy(
                      others = (family_structure.others :+ ("member", participant))
                    )
                  }
                }
              }
              case Some(relationships) => {
                if(relationships.contains("father")) {
                  family_structure.father match {
                    case Some(_) => {
                      family_structure.copy(
                        others = family_structure.others :+ ("father", participant)
                      )
                    }
                    case None => family_structure.copy(father = Some(participant))
                  }
                }
                else if(relationships.contains("mother")) {
                  family_structure.mother match {
                    case Some(_) => {
                      family_structure.copy(
                        others = family_structure.others :+ ("mother", participant)
                      )
                    }
                    case None => family_structure.copy(mother = Some(participant))
                  }

                }
                else if(relationships.contains("child")) {
                  if (participant.isProband.isDefined && participant.isProband.get)
                    family_structure.copy(proband =Some(participant))
                  else
                    family_structure.copy(others = (family_structure.others :+ ("child", participant)))
                }
                else {
                  family_structure.copy(
                    others = family_structure.others :+ (relationships.toString(), participant)
                  )
                }
              }//end of Some(relationships)
            }//end of familyRelationship.get(participant.kfId.get) match {
          }}// end of family.foldLeft

        familyStructure match {
          case FamilyStructure(Some(father), Some(mother), Some(proband), Seq()) => {
            val sharedHpoIds = getSharedHpoIds(Seq(father, mother, proband))
            val family_availableDataTypes = getAvailableDataTypes(Seq(father, mother, proband), mapOfAvailableDataTypes)

            val composition =
              FamilyComposition_ES(
                composition = Some("trio"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(mother, "mother", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                )
              )

            Seq(
              father.copy(family = Some(
                Family_ES(
                  familyId = father.familyId.get,
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(father), mapOfAvailableDataTypes)
              ),
              mother.copy(family = Some(
                Family_ES(
                  familyId = mother.familyId.get,
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(mother), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            )

          }//end of case FamilyStructure(Some(father), Some(mother), Some(proband), Seq())
          case FamilyStructure(Some(father), Some(mother), Some(proband), Seq(head, tail @ _*)) => {

            val members = Seq(father, mother, proband, head._2) ++ tail.map(_._2)
            val sharedHpoIds = getSharedHpoIds(members)
            val family_availableDataTypes = getAvailableDataTypes(members, mapOfAvailableDataTypes).toSet.toSeq

            val composition =
              FamilyComposition_ES(
                composition = Some("trio+"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(mother, "mother", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                ) ++ (Seq(head) ++ tail).map(m => {
                  getFamilyMemberFromParticipant(m._2, m._1, sharedHpoIds, mapOfAvailableDataTypes)
                })
              )

            Seq(
              father.copy(family = Some(
                Family_ES(
                  familyId = father.familyId.get,
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(father), mapOfAvailableDataTypes)
              ),
              mother.copy(family = Some(
                Family_ES(
                  familyId = mother.familyId.get,
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(mother), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            ) ++ (Seq(head) ++ tail).map(m => {
              m._2.copy(
                family = Some(
                  Family_ES(
                    familyId = m._2.familyId.get,
                    familyCompositions = Seq(composition),
                    fatherId = father.kfId,
                    motherId = mother.kfId
                  )
                ),
                availableDataTypes = getAvailableDataTypes(Seq(m._2), mapOfAvailableDataTypes)
              )
            })
          }//end of case FamilyStructure(Some(father), Some(mother), Some(proband), Seq(head, tail @ _*))
          case FamilyStructure(Some(father), None, Some(proband), Seq()) => {
            val sharedHpoIds = getSharedHpoIds(Seq(father, proband))
            val family_availableDataTypes = getAvailableDataTypes(Seq(father, proband), mapOfAvailableDataTypes)

            val composition =
              FamilyComposition_ES(
                composition = Some("duo"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                )
              )

            Seq(
              father.copy(family = Some(
                Family_ES(
                  familyId = father.familyId.get,
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(father), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            )

          }//end of case FamilyStructure(Some(father), None, Some(proband), seq)
          case FamilyStructure(Some(father), None, Some(proband), Seq(head, tail @ _*)) => {
            val members = Seq(father, proband, head._2) ++ tail.map(_._2)
            val sharedHpoIds = getSharedHpoIds(members)
            val family_availableDataTypes = getAvailableDataTypes(members, mapOfAvailableDataTypes)

            val composition =
              FamilyComposition_ES(
                composition = Some("duo+"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(father, "father", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                ) ++ (Seq(head) ++ tail).map(m => {
                  getFamilyMemberFromParticipant(m._2, m._1, sharedHpoIds, mapOfAvailableDataTypes)
                })
              )

            Seq(
              father.copy(family = Some(
                Family_ES(
                  familyId = father.familyId.get,
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(father), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            ) ++ (Seq(head) ++ tail).map(m => {
              m._2.copy(
                family = Some(
                  Family_ES(
                    familyId = m._2.familyId.get,
                    familyCompositions = Seq(composition),
                    fatherId = father.kfId
                  )
                ),
                availableDataTypes = getAvailableDataTypes(Seq(m._2), mapOfAvailableDataTypes)
              )
            })
          }//end of case FamilyStructure(Some(father), None, Some(proband), seq)
          case FamilyStructure(None, Some(mother), Some(proband), Seq()) => {
            val sharedHpoIds = getSharedHpoIds(Seq(mother, proband))
            val family_availableDataTypes = getAvailableDataTypes(Seq(mother, proband), mapOfAvailableDataTypes)

            val composition =
              FamilyComposition_ES(
                composition = Some("duo"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(mother, "mother", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                )
              )

            Seq(
              mother.copy(family = Some(
                Family_ES(
                  familyId = mother.familyId.get,
                  familyCompositions = Seq(composition),
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(mother), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(composition),
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            )

          }//end of case FamilyStructure(None, Some(mother), Some(proband), seq)
          case FamilyStructure(None, Some(mother), Some(proband), Seq(head, tail @ _*)) => {
            val members = Seq(mother, proband, head._2) ++ tail.map(_._2)
            val sharedHpoIds = getSharedHpoIds(members)
            val family_availableDataTypes = getAvailableDataTypes(members, mapOfAvailableDataTypes)

            val composition =
              FamilyComposition_ES(
                composition = Some("duo+"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(mother, "mother", sharedHpoIds, mapOfAvailableDataTypes),
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                ) ++ (Seq(head) ++ tail).map(m => {
                  getFamilyMemberFromParticipant(m._2, m._1, sharedHpoIds, mapOfAvailableDataTypes)
                })
              )

            Seq(
              mother.copy(family = Some(
                Family_ES(
                  familyId = mother.familyId.get,
                  familyCompositions = Seq(composition),
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(mother), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(composition),
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            ) ++ (Seq(head) ++ tail).map(m => {
              m._2.copy(
                family = Some(
                  Family_ES(
                    familyId = m._2.familyId.get,
                    familyCompositions = Seq(composition),
                    motherId = mother.kfId
                  )
                ),
                availableDataTypes = getAvailableDataTypes(Seq(m._2), mapOfAvailableDataTypes)
              )
            })
          }//end of case FamilyStructure(None, Some(mother), Some(proband), seq)
          case FamilyStructure(None, None, Some(proband), Seq()) => {
            val members = Seq(proband)
            val sharedHpoIds = getSharedHpoIds(members)
            val family_availableDataTypes = getAvailableDataTypes(members, mapOfAvailableDataTypes)

            val composition =
              FamilyComposition_ES(
                composition = Some("proband-only"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                )
              )

            Seq(

              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(composition)
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            )
          }//end of FamilyStructure(None, None, Some(proband), Seq())
          case FamilyStructure(None, None, Some(proband), Seq(head, tail @ _*)) => {
            val members = Seq(proband, head._2) ++ tail.map(_._2)
            val sharedHpoIds = getSharedHpoIds(members)
            val family_availableDataTypes = getAvailableDataTypes(members, mapOfAvailableDataTypes)

            val composition =
              FamilyComposition_ES(
                composition = Some("other"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = Seq(
                  getFamilyMemberFromParticipant(proband, "child", sharedHpoIds, mapOfAvailableDataTypes)
                ) ++ (Seq(head) ++ tail).map(m => {
                  getFamilyMemberFromParticipant(m._2, m._1, sharedHpoIds, mapOfAvailableDataTypes)
                })
              )

            Seq(

              proband.copy(family = Some(
                Family_ES(
                  familyId = proband.familyId.get,
                  familyCompositions = Seq(composition)
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            ) ++ (Seq(head) ++ tail).map(m => {
              m._2.copy(
                family = Some(
                  Family_ES(
                    familyId = m._2.familyId.get,
                    familyCompositions = Seq(composition)
                  )
                ),
                availableDataTypes = getAvailableDataTypes(Seq(m._2), mapOfAvailableDataTypes)
              )
            })
          }//end of case FamilyStructure(None, None, Some(proband), seq)
          case _ => {

            val sharedHpoIds = getSharedHpoIds(family)
            val family_availableDataTypes = getAvailableDataTypes(family, mapOfAvailableDataTypes)
            val composition =
              FamilyComposition_ES(
                composition = Some("other"),
                sharedHpoIds = sharedHpoIds,
                availableDataTypes = family_availableDataTypes,
                familyMembers = family.map(member => {
                  getFamilyMemberFromParticipant(member, "member", sharedHpoIds, mapOfAvailableDataTypes)
                })
              )

            family.map(member => {
              member.copy(
                family = Some(
                  Family_ES(
                    familyId = member.familyId.get,
                    familyCompositions = Seq(composition)
                  )
                ),
                availableDataTypes = getAvailableDataTypes(Seq(member), mapOfAvailableDataTypes)
              )
            })

          }//end case _

        }//end of familyStructure match {

      }//end case Some(id) =>
    }//end of familyId match
  }

  def getFamilyMemberFromParticipant(participant: Participant_ES, relationship:String, familySharedHpoIds:Seq[String], mapOfAvailableDataTypes: Map[String, Seq[String]]): FamilyMember_ES = {
    FamilyMember_ES(
      kfId = participant.kfId,
      createdAt = participant.createdAt,
      modifiedAt = participant.modifiedAt,
      isProband = participant.isProband,
      availableDataTypes = mapOfAvailableDataTypes.get(participant.kfId.get) match {
        case Some(types) => types.toSet.toSeq
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

//    participants.flatMap(participant => {
//      mapOfAvailableDataTypes.get(participant.kfId.get) match {
//        case Some(seq) => seq
//        case None => Seq.empty
//      }
//    }).toSet.toSeq

    seqOfDataTypes.tail.foldLeft(seqOfDataTypes.head){(left, right) => {
      left.intersect(right)
    }}.toSet.toSeq
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