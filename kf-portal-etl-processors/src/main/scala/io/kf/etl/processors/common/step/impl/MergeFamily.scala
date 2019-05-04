package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{FamilyComposition_ES, FamilyMember_ES, Family_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EFamilyRelationship
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.context.StepContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

class MergeFamily(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.spark.implicits._

    val availableDataTypesBroadcast = calculateAvailableDataTypes(ctx.entityDataset)
    val flattenedFamilyRelationshipBroadcast = getFlattenedFamilyRelationship(ctx.entityDataset)

    participants
      .groupByKey(_.familyId)
      .flatMapGroups((family_id, iterator) => {
        MergeFamily.deduceFamilyCompositions(family_id, iterator.toSeq, flattenedFamilyRelationshipBroadcast, availableDataTypesBroadcast)
      })
  }

  def calculateAvailableDataTypes(entityDataset: EntityDataSet): Broadcast[Map[String, Seq[String]]] = {
    import ctx.spark.implicits._
    import entityDataset._
    val datatypeByParticipant = participants
      .join(biospecimens, participants("kfId") === biospecimens("participantId"))
      .join(biospecimenGenomicFiles, biospecimens("kfId") === biospecimenGenomicFiles("biospecimenId"))
      .join(genomicFiles, biospecimenGenomicFiles("genomicFileId") === genomicFiles("kfId"))
      .select(participants("kfId") as "participantId", genomicFiles("dataType") as "dataType")
      .groupBy($"participantId").agg(collect_list("dataType") as "dataTypes")
      .as[(String, Seq[String])]
      .collect()
      .toMap

    val distinctDatatypeByParticipant = datatypeByParticipant.map { case (k, v) => k -> v.distinct }

    ctx.spark.sparkContext.broadcast[Map[String, Seq[String]]](distinctDatatypeByParticipant)

  }

  private def getFlattenedFamilyRelationship(entityDataset: EntityDataSet): Broadcast[Map[String, Set[String]]] = {
    /**
      * flattenedFamilyRelationship is a map
      * key  : participant id
      * value: Set of relation types as lowercase strings
      */
    import ctx.spark.implicits._

    ctx.spark.sparkContext.broadcast(
      entityDataset.familyRelationships

        // Make a copy of every family relationship so we have an instance with each family member as participant 1
        .flatMap(tf => {
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
      })

        // group the relationship objects for each participant
        .groupByKey(_.participant1.get)

        // for each group, create a tuple of form (participant_id, Seq(relation type))
        // This also applies the toLowerCase modifier to the relationship text
        .mapGroups((participant_id, iterator) => {
        (
          participant_id,
          iterator.collect {
            case tf: EFamilyRelationship if tf.participant1ToParticipant2Relation.isDefined => {
              tf.participant1ToParticipant2Relation.get.toLowerCase
            }
          }.toSet
        )
      })
        .collect()

        // Uncertain why this groupBy is called, shouldn't we already have the groups correct after our mapGroups above?
        .groupBy(_._1)

        // Flatten the tuple._2 into a list of relation types
        .map(tuple => {
        (
          tuple._1,
          tuple._2.flatMap(_._2)
        )
      })

        // Call toSet on the tuple values to remove duplicates
        .map(tuple => (tuple._1, tuple._2.toSet))
    )
  }
}

object MergeFamily {

  case class FamilyStructure(father: Option[Participant_ES] = None,
                             mother: Option[Participant_ES] = None,
                             proband: Option[Participant_ES] = None,
                             others: Seq[(String, Participant_ES)] = Seq.empty
                            )

  class ProbandMissingInFamilyException extends Exception("Family has no proband child!")

  /*
    familyRelationship map:
    key  : participant id
    value: list of relationship in the family, for example, father, mother, child
   */

  def deduceFamilyCompositions(familyId: Option[String],
                               family: Seq[Participant_ES],
                               familyRelationship_broadcast: Broadcast[Map[String, Set[String]]],
                               availableDataTypes_broadcast: Broadcast[Map[String, Seq[String]]]
                              ): Seq[Participant_ES] = {
    val familyRelationship = familyRelationship_broadcast.value
    val mapOfAvailableDataTypes = availableDataTypes_broadcast.value

    familyId match {

      case None => family.map(participant => {

        val members = Seq(participant)
        val sharedHpoIds = getSharedHpoIds(members)
        val family_availableDataTypes = getAvailableDataTypes(members, mapOfAvailableDataTypes)

        val composition = participant.isProband match {
          case Some(true) => "proband-only"
          case _ => "other"
        }

        val familyMembers = Seq(
          getFamilyMemberFromParticipant(participant, "", sharedHpoIds, mapOfAvailableDataTypes)
        )

        val familyComposition =
          FamilyComposition_ES(
            composition = Some(composition),
            sharedHpoIds = sharedHpoIds,
            availableDataTypes = family_availableDataTypes,
            familyMembers = familyMembers
          )

        val family =
          Family_ES(
            familyCompositions = Seq(familyComposition)
          )

        participant.copy(
          family = Some(family),
          availableDataTypes = getAvailableDataTypes(Seq(participant), mapOfAvailableDataTypes)
        )

      })

      case Some(id) => {
        val familyStructure =
          family
            .foldLeft(FamilyStructure()) { (family_structure, participant) => {

              familyRelationship.get(participant.kfId.get) match {

                // None case means no family
                case None => {
                  participant.isProband match {
                    case Some(isProband) if isProband => {
                      family_structure.copy(proband = Some(participant))
                    }
                    case _ => {
                      family_structure.copy(
                        others = family_structure.others :+ ("member", participant)
                      )
                    }
                  }
                }

                case Some(relationships) => {

                  if (relationships.contains("father")) {
                    family_structure.father match {
                      case Some(_) => {
                        family_structure.copy(
                          others = family_structure.others :+ ("father", participant)
                        )
                      }
                      case None => family_structure.copy(father = Some(participant))
                    }
                  }

                  else if (relationships.contains("mother")) {
                    family_structure.mother match {
                      case Some(_) => {
                        family_structure.copy(
                          others = family_structure.others :+ ("mother", participant)
                        )
                      }
                      case None => family_structure.copy(mother = Some(participant))
                    }

                  }

                  else if (relationships.contains("child")) {
                    if (participant.isProband.isDefined && participant.isProband.get)
                      family_structure.copy(proband = Some(participant))
                    else
                      family_structure.copy(others = family_structure.others :+ ("child", participant))
                  }

                  else {
                    family_structure.copy(
                      others = family_structure.others :+ (relationships.toString(), participant)
                    )
                  }
                } //end of Some(relationships)
              } //end of familyRelationship.get(participant.kfId.get) match {
            }
            } // end of family.foldLeft

        familyStructure match {
          // trio = mother, father, proband
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
                  familyId = Some(father.familyId.get),
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(father), mapOfAvailableDataTypes)
              ),
              mother.copy(family = Some(
                Family_ES(
                  familyId = Some(mother.familyId.get),
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(mother), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = Some(proband.familyId.get),
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            )

          } //end of case FamilyStructure(Some(father), Some(mother), Some(proband), Seq())

          // trio+ = mother, father, proband, other
          case FamilyStructure(Some(father), Some(mother), Some(proband), Seq(head, tail@_*)) => {

            val members = Seq(father, mother, proband, head._2) ++ tail.map(_._2)
            val sharedHpoIds = getSharedHpoIds(members)
            val family_availableDataTypes = getAvailableDataTypes(members, mapOfAvailableDataTypes).distinct

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
                  familyId = Some(father.familyId.get),
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(father), mapOfAvailableDataTypes)
              ),
              mother.copy(family = Some(
                Family_ES(
                  familyId = Some(mother.familyId.get),
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId,
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(mother), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = Some(proband.familyId.get),
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
                    familyId = Some(m._2.familyId.get),
                    familyCompositions = Seq(composition),
                    fatherId = father.kfId,
                    motherId = mother.kfId
                  )
                ),
                availableDataTypes = getAvailableDataTypes(Seq(m._2), mapOfAvailableDataTypes)
              )
            })
          } //end of case FamilyStructure(Some(father), Some(mother), Some(proband), Seq(head, tail @ _*))

          // duo = father, proband
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
                  familyId = Some(father.familyId.get),
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(father), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = Some(proband.familyId.get),
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            )

          } //end of case FamilyStructure(Some(father), None, Some(proband), seq)

          // duo+ = father, proband, other
          case FamilyStructure(Some(father), None, Some(proband), Seq(head, tail@_*)) => {
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
                  familyId = Some(father.familyId.get),
                  familyCompositions = Seq(composition),
                  fatherId = father.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(father), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = Some(proband.familyId.get),
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
                    familyId = Some(m._2.familyId.get),
                    familyCompositions = Seq(composition),
                    fatherId = father.kfId
                  )
                ),
                availableDataTypes = getAvailableDataTypes(Seq(m._2), mapOfAvailableDataTypes)
              )
            })
          } //end of case FamilyStructure(Some(father), None, Some(proband), seq)

          // duo = mother, proband
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
                  familyId = Some(mother.familyId.get),
                  familyCompositions = Seq(composition),
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(mother), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = Some(proband.familyId.get),
                  familyCompositions = Seq(composition),
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            )

          } //end of case FamilyStructure(None, Some(mother), Some(proband), seq)

          // duo+ = mother, proband, other
          case FamilyStructure(None, Some(mother), Some(proband), Seq(head, tail@_*)) => {
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
                  familyId = Some(mother.familyId.get),
                  familyCompositions = Seq(composition),
                  motherId = mother.kfId
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(mother), mapOfAvailableDataTypes)
              ),
              proband.copy(family = Some(
                Family_ES(
                  familyId = Some(proband.familyId.get),
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
                    familyId = Some(m._2.familyId.get),
                    familyCompositions = Seq(composition),
                    motherId = mother.kfId
                  )
                ),
                availableDataTypes = getAvailableDataTypes(Seq(m._2), mapOfAvailableDataTypes)
              )
            })
          } //end of case FamilyStructure(None, Some(mother), Some(proband), seq)

          // proband-only = proband
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
                  familyId = Some(proband.familyId.get),
                  familyCompositions = Seq(composition)
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            )
          } //end of FamilyStructure(None, None, Some(proband), Seq())

          // other = proband, other
          case FamilyStructure(None, None, Some(proband), Seq(head, tail@_*)) => {

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
                  familyId = Some(proband.familyId.get),
                  familyCompositions = Seq(composition)
                )
              ),
                availableDataTypes = getAvailableDataTypes(Seq(proband), mapOfAvailableDataTypes)
              )
            ) ++ (Seq(head) ++ tail).map(m => {
              m._2.copy(
                family = Some(
                  Family_ES(
                    familyId = Some(m._2.familyId.get),
                    familyCompositions = Seq(composition)
                  )
                ),
                availableDataTypes = getAvailableDataTypes(Seq(m._2), mapOfAvailableDataTypes)
              )
            })
          } //end of case FamilyStructure(None, None, Some(proband), seq)

          // other = default (fall through)
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
                    familyId = Some(member.familyId.get),
                    familyCompositions = Seq(composition)
                  )
                ),
                availableDataTypes = getAvailableDataTypes(Seq(member), mapOfAvailableDataTypes)
              )
            })

          } //end case _

        } //end of familyStructure match {

      } //end case Some(id) =>
    } //end of familyId match
  }

  def getFamilyMemberFromParticipant(participant: Participant_ES,
                                     relationship: String,
                                     familySharedHpoIds: Seq[String],
                                     mapOfAvailableDataTypes: Map[String, Seq[String]]): FamilyMember_ES = {

    val availableDataTypes = mapOfAvailableDataTypes.get(participant.kfId.get) match {
      case Some(types) => types.toSet.toSeq
      case None => Seq.empty
    }


    FamilyMember_ES(
      kfId = participant.kfId,
      isProband = participant.isProband,
      availableDataTypes = availableDataTypes,
      phenotype = participant.phenotype,
      race = participant.race,
      ethnicity = participant.ethnicity,
      relationship = if (relationship.isEmpty()) None else Some(relationship)
    )
  }

  def getAvailableDataTypes(participants: Seq[Participant_ES], mapOfAvailableDataTypes: Map[String, Seq[String]]): Seq[String] = {

    val participantKeys: Seq[String] = participants.flatMap(_.kfId)
    mapOfAvailableDataTypes.filterKeys(participantKeys.contains).values.flatten.toSet.toSeq

  }

  def getSharedHpoIds(participants: Seq[Participant_ES]): Seq[String] = {
    participants.tail.foldLeft(
      participants.head.phenotype match {
        case Some(pt) => pt.hpoPhenotypeObserved
        case None => Seq.empty[String]
      }
    ) { (pts, participant) => {
      participant.phenotype match {
        case Some(pt) => pt.hpoPhenotypeObserved
        case None => Seq.empty[String]
      }
    }
    }
  }

}