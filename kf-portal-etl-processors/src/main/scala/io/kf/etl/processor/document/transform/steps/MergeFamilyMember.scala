package io.kf.etl.processor.document.transform.steps

import io.kf.etl.model.{Family, FamilyData, FamilyMember, Participant}
import io.kf.etl.processor.common.ProcessorCommonDefinitions.{DS_FAMILYRELATIONSHIP, FamilyMemberRelation}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

class MergeFamilyMember(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]] {
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {

    import ctx.parentContext.sparkSession.implicits._

    val family_relations = familyMemberRelationship(participants, ctx.dbTables.familyRelationship)

    participants.joinWith(ctx.participant2GenomicFiles, col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(files) => tuple._1.copy(availableDataTypes = files.dataTypes)
        case None => tuple._1
      }
    }).joinWith(family_relations, col("kfId"), "left").groupByKey(tuple => {
      tuple._1.kfId
    }).mapGroups((id, iterator) => {
      val list = iterator.toList.filter(_._2 != null)

      buildFamily(list(0)._1, list.map(_._2))

    })
  }

  /**
    * this method returns participant and his relatives, where a participant is represented by only kfId, a relative is represented by a instance of Participant. Plus also the relationship from a relative to a participant
    * @param left
    * @param right
    * @return
    */
  private def familyMemberRelationship(left: Dataset[Participant], right: DS_FAMILYRELATIONSHIP): Dataset[FamilyMemberRelation] = {
    import ctx.parentContext.sparkSession.implicits._

    left.joinWith(right, left.col("kfId") === right.col("relativeId")).map(tuple => {

      FamilyMemberRelation(
        tuple._2.participantId,
        tuple._1,
        tuple._2.relativeToParticipantRelation
      )
    })
  }

  private def buildFamily(participant: Participant, relatives: Seq[FamilyMemberRelation]): Participant = {

    def buildFamilyData(composition:String, fmr: FamilyMemberRelation): FamilyData = {
      FamilyData(
        availableDataTypes = participant.availableDataTypes.intersect(fmr.relative.availableDataTypes),
        composition = composition,
        sharedHpoIds = {
          participant.phenotype.flatMap(pt => {
            pt.hpo match {
              case None => None
              case Some(hpo) => {
                Some(hpo.hpoIds)
              }
            }
          }).toList.flatten.intersect(
            fmr.relative.phenotype.flatMap(pt => {
              pt.hpo match {
                case None => None
                case (Some(hpo)) => {
                  Some(hpo.hpoIds)
                }
              }
            }).toList.flatten
          )
        }
      )
    }

    val father_mother = ".*(father|mother).*".r
    val twin = ".*twin.*".r

    relatives.size match {
      case 0 => participant
      case _ => {
        val familyData =
          relatives.map(relative => {
            relative.relation match {
              case Some(relation) => {
                relation.toLowerCase match {
                  case father_mother(c) => {
                    ("parent", buildFamilyData("partial_trio", relative))
                  }
                  case twin(c) => {
                    ("twin", buildFamilyData("twin", relative))
                  }
                  case _ => {
                    ("other",buildFamilyData("other", relative))
                  }
                }

              }
              case None => {
                // by default is "other"
                ("other",buildFamilyData("other", relative))
              }
            }
          }).groupBy(_._1).flatMap(tuple => {
            tuple._1 match {
              case "parent" => {
                tuple._2.size match {
                  case 1 => tuple._2.map(_._2)
                  case _ => {
                    val fds = tuple._2.map(_._2)
                    Seq(
                      fds.tail.foldLeft(fds(0).copy(composition = "complete_trio")){(left, right) => {
                        left.copy(
                          availableDataTypes = left.availableDataTypes.intersect(right.availableDataTypes),
                          sharedHpoIds = left.sharedHpoIds.intersect(right.sharedHpoIds)
                        )
                      }}
                    )
                  }
                }
              }
              case _ => tuple._2.map(_._2)
            }
          }).toSeq

        val familySharedHPOIds =
          familyData.tail.foldLeft(familyData(0).sharedHpoIds){(seq, fd) => {
            seq.intersect(fd.sharedHpoIds)
          }}

        participant.copy(
          family = Some(
            Family(
              familyId = participant.family.get.familyId,
              familyData = familyData,
              familyMembers = {
                relatives.map(p => {
                  FamilyMember(
                    kfId = p.relative.kfId,
                    uuid = p.relative.uuid,
                    createdAt = p.relative.createdAt,
                    modifiedAt = p.relative.modifiedAt,
                    isProband = p.relative.isProband,
                    availableDataTypes = p.relative.availableDataTypes,
                    phenotype = p.relative.phenotype.map(pt => {
                      pt.copy(hpo = Some(
                        pt.hpo.get.copy(sharedHpoIds = familySharedHPOIds)
                      ))
                    }),
                    studies = p.relative.studies,
                    race = p.relative.race,
                    ethnicity = p.relative.ethnicity,
                    relationship = p.relation
                  )
                })
              }
            )
          )
        )
      }
    }
  }

}