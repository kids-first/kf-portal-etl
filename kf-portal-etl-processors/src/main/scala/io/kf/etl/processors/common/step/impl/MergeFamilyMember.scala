package io.kf.etl.processors.common.step.impl

import io.kf.etl.dbschema.TFamilyRelationship
import io.kf.etl.model.{Family, FamilyData, FamilyMember, Participant}
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.impl.MergeFamilyMember.FamilyMemberRelation
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeFamilyMember(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]] {
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {

    import ctx.spark.implicits._

    val flattenedFamilyRelationship =
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
      })

    val family_relations = MergeFamilyMemberHelper.familyMemberRelationship(ctx, participants, flattenedFamilyRelationship)

    val ds =
    participants.joinWith(ctx.dbTables.participantGenomicFile, participants.col("kfId") === ctx.dbTables.participantGenomicFile.col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(files) => tuple._1.copy(availableDataTypes = files.dataTypes)
        case None => tuple._1
      }
    })

    ds.joinWith(family_relations, ds.col("kfId") === family_relations.col("kfId"), "left").groupByKey(tuple => {
      tuple._1.kfId
    }).mapGroups((id, iterator) => {

      val list = iterator.toList
      val filteredList = list.filter(_._2 != null)

      if(filteredList.size == 0) {
        list(0)._1
      }
      else{
        MergeFamilyMemberHelper.buildFamily(filteredList(0)._1, filteredList.map(_._2))
      }

    })
  }

}

object MergeFamilyMember{
  case class FamilyMemberRelation(kfId:String, relative: Participant, relativeToParcitipantRelation: Option[String])

}


// helper class is defined for avoiding to make MergeFamilyMember serializable
object MergeFamilyMemberHelper {
  /**
    * this method returns participant and his relatives, where a participant is represented by only kfId, a relative is represented by a instance of Participant. Plus also the relationship from a relative to a participant
    * @param left
    * @param right
    * @return
    */
  def familyMemberRelationship(ctx: StepContext, left: Dataset[Participant], right: Dataset[TFamilyRelationship]): Dataset[FamilyMemberRelation] = {
    import ctx.spark.implicits._

    left.joinWith(right, left.col("kfId") === right.col("relativeId")).map(tuple => {

      FamilyMemberRelation(
        tuple._2.participantId,
        tuple._1,
        tuple._2.relativeToParticipantRelation
      )
    })
  }

  def buildFamily(participant: Participant, relatives: Seq[FamilyMemberRelation]): Participant = {

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
            relative.relativeToParcitipantRelation match {
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

        participant.family match {
          case Some(family) => {
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
                        study = p.relative.study,
                        race = p.relative.race,
                        ethnicity = p.relative.ethnicity,
                        relationship = p.relativeToParcitipantRelation
                      )
                    })
                  }
                )
              )
            )
          }
          case None => participant
        }

      }
    }
  }
}