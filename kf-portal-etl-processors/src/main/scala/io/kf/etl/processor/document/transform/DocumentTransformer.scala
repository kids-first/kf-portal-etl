package io.kf.etl.processor.document.transform

import io.kf.etl.processor.common.ProcessorCommonDefinitions._
import io.kf.etl.model._
import io.kf.etl.processor.document.context.DocumentContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

class DocumentTransformer(val context: DocumentContext) {

  def transform(input: DatasetsFromDBTables): Dataset[DocType] = {
    ???
  }

  private def buildFamily(all: DatasetsFromDBTables):Unit = {
    import context.sparkSession.implicits._

    val mergedParticipants = merge_Demographic_Diagnosis_Phenotype_Study_into_Participant(all).cache()

    val datatypes = collectDataTypesForParticipant(all).cache()

    val family_relations = familyMemberRelationship(mergedParticipants, all.familyRelationship)

    val participants =
      mergedParticipants.joinWith(datatypes, col("kfId"), "left").map(tuple => {
        Option(tuple._2) match {
          case Some(types) => tuple._1.copy(availableDataTypes = types.datatypes)
          case None => tuple._1
        }
      }).cache()

    participants.joinWith(family_relations, col("kfId"), "left").groupByKey(tuple => {
      tuple._1.kfId
    }).mapGroups((id, iterator) => {
      val list = iterator.toList

      val datatypes_in_family = (list.flatMap(_._2.relative.availableDataTypes) ++ list(0)._1.availableDataTypes).toSet.toSeq

      list(0)._1.family.get.copy(
        familyMembers = {
          list.map(p => {
            FamilyMember(
              kfId = p._2.relative.kfId,
              uuid = p._2.relative.uuid,
              createdAt = p._2.relative.createdAt,
              modifiedAt = p._2.relative.modifiedAt,
              isProband = p._2.relative.isProband,
              availableDataTypes = p._2.relative.availableDataTypes,
              phenotype = p._2.relative.phenotype,
              studies = p._2.relative.studies,
              race = p._2.relative.race,
              ethnicity = p._2.relative.ethnicity,
              relationship = p._2.relation
            )
          })
        },
        familyData = Some(
          FamilyData(
            availableDataTypes = datatypes_in_family,
            composition = ???,
            sharedHpoIds = ???
          )
        )
      )
    })

  }

  private def merge_Demographic_Diagnosis_Phenotype_Study_into_Participant(all: DatasetsFromDBTables): Dataset[Participant] = {
    import context.sparkSession.implicits._
    all.participant.createOrReplaceGlobalTempView("P")
    all.demographic.createOrReplaceGlobalTempView("DG")
    all.diagnosis.createOrReplaceGlobalTempView("D")
    all.phenotype.createOrReplaceGlobalTempView("PY")
    all.study.createOrReplaceGlobalTempView("S")

    val sql1 = "select P.kfId, P.uuid, P.createdAt, P.modifiedAt, P.familyId, P.isProband, P.consentType, DG.race, DG.ethnicity, DG.gender from P left join DG on P.kfId = DG.participantId"

    val participant_demographic =
    context.sparkSession.sql(sql1).map(row => {
      Participant(
        kfId = row.getString(0),
        uuid = row.getString(1),
        createdAt = row.getString(2),
        modifiedAt = row.getString(3),
        family = {
          val value = row.getString(4)
          value match {
            case "null" => None
            case _ => Some(Family(familyId = value))
          }
        },
        isProband = {
          val value = row.getString(5)
          value match {
            case "null" => None
            case _ => Some(value.toBoolean)
          }
        },
        consentType = {
          val value = row.getString(6)
          value match {
            case "null" => None
            case _ => Some(value)
          }
        },
        race = {
          val value = row.getString(7)
          value match {
            case "null" => None
            case _ => Some(value)
          }
        },
        ethnicity = {
          val value = row.getString(8)
          value match {
            case "null" => None
            case _ => Some(value)
          }
        },
        gender = {
          val value = row.getString(9)
          value match {
            case "null" => None
            case _ => Some(value)
          }
        }
      )
    })

    val sql2 = "select P.kfId, D.kfId, D.uuid, D.createdAt, D.modifiedAt, D.diagnosis, D.agetAtEventDays, D.tumorLocation, D.diagnosisCategory from P left join D on P.kfId = D.participantId"
    val participant_diagnosis =
    context.sparkSession.sql(sql2).groupByKey(_.getString(0)).mapGroups((id, iterator) => {
      val diagnoses =
        iterator.toList.map(row => {
          Diagnosis(
            kfId = row.getString(1),
            uuid = row.getString(2),
            createdAt = row.getString(3),
            modifiedAt = row.getString(4),
            diagnosis = {
              val value = row.getString(5)
              value match {
                case "null" => None
                case _ => Some(value)
              }
            },
            ageAtEventDays = {
              val value = row.getString(6)
              value match {
                case "null" => None
                case _ => Some(value.toLong)
              }
            },
            tumorLocation = {
              val value = row.getString(7)
              value match {
                case "null" => None
                case _ => Some(value)
              }
            },
            diagnosisCategory = {
              val value = row.getString(8)
              value match {
                case "null" => None
                case _ => Some(value)
              }
            }
          )
        })
      Participant(
        kfId = id,
        uuid = "placeholder",
        createdAt = "placeholder",
        modifiedAt = "placeholder",
        diagnoses = diagnoses
      )
    })

    val sql3 = "select P.kfId, PY.kfId, PY.uuid, PY.createdAt, PY.modifiedAt, PY.phenotype, PY.hpo_id, PY.ageAtEventDays, PY.observed from P left join PY on P.kfId = PY.participantId"
    val participant_phenotype =
    context.sparkSession.sql(sql3).groupByKey(_.getString(0)).mapGroups((id, iterator) => {
      val phenotypes =
      iterator.toList.map(row => {
        Phenotype(
          // add more details here
        )
      })
      Participant(
        kfId = id,
        uuid = "placeholder",
        createdAt = "placeholder",
        modifiedAt = "placeholder",
        phenotype = phenotypes
      )
    })

    val sql4 = "select P.kfId, S.kfId, S.uuid, S.createdAt, S.modifiedAt, S.dataAccessAuthority, S.externalId, S.version, S.name, S.attribution from P left join S on P.kfId=S.participantId"
    val participant_study =
    context.sparkSession.sql(sql4).groupByKey(_.getString(0)).mapGroups((id, iterator) => {
      val studies =
      iterator.toList.map(row => {
        Study(
          kfId = row.getString(1),
          uuid = row.getString(2),
          createdAt = row.getString(3),
          modifiedAt = row.getString(4),
          dataAccessAuthority = {
            val value = row.getString(5)
            value match {
              case "null" => None
              case _ => Some(value)
            }
          },
          externalId = {
            val value = row.getString(6)
            value match {
              case "null" => None
              case _ => Some(value)
            }
          },
          version = {
            val value = row.getString(7)
            value match {
              case "null" => None
              case _ => Some(value)
            }
          },
          name = {
            val value = row.getString(8)
            value match {
              case "null" => None
              case _ => Some(value)
            }
          },
          attribution = {
            val value = row.getString(9)
            value match {
              case "null" => None
              case _ => Some(value)
            }
          }
        )

      })
      Participant(
        kfId = id,
        uuid = "placeholder",
        createdAt = "placeholder",
        modifiedAt = "placeholder",
        studies = studies
      )
    })

    participant_demographic.joinWith(participant_diagnosis, col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(p) => tuple._1.copy(diagnoses = p.diagnoses)
        case None => tuple._1
      }

    }).joinWith(participant_phenotype, col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(p) => tuple._1.copy(phenotype = p.phenotype)
        case None => tuple._1
      }
    }).joinWith(participant_study, col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(p) => tuple._1.copy(studies = p.studies)
        case None => tuple._1
      }
    })

  }

  private def collectDataTypesForParticipant(all: DatasetsFromDBTables): Dataset[ParticipantDataTypes] = {

    val sql =
      """
          select Participant.kfId, AA.dataType from Participant left join (
            (select Sample.kfId, BB.dataType from Sample left join (
              (select Aliquot.kfId , CC.dataType from Aliquot left join (
                (select SequencingExperiment.kfId, GenomicFile.dataType from SequencingExperiment left join GenomicFile on SequencingExperiment.kfId = GenomicFile.kfId) as CC
              ) on Aliquot.kfId = CC.kfId) as BB
            ) on Sample.kfId = BB.kfId) as AA
          ) on Participant.kf = AA.kfId
      """.stripMargin

    all.participant.createOrReplaceGlobalTempView("Participant")
    all.sample.createOrReplaceGlobalTempView("Sample")
    all.aliquot.createOrReplaceGlobalTempView("Aliquot")
    all.sequencingExperiment.createOrReplaceGlobalTempView("SequencingExperiment")
    all.genomicFile.createOrReplaceGlobalTempView("GenomicFile")

    import context.sparkSession.implicits._
    context.sparkSession.sql(sql).groupByKey(_.getString(0)).mapGroups((id, iterator) => {
      ParticipantDataTypes(
        id,
        iterator.map(_.getString(1)).toSet.toSeq
      )
    })

  }

  /**
    * this method returns participant and his relatives, where a participant is represented by only kfId, a relative is represented by a instance of Participant. Plus also the relationship from a relative to a participant
    * @param left
    * @param right
    * @return
    */
  private def familyMemberRelationship(left: Dataset[Participant], right: DS_FAMILYRELATIONSHIP): Dataset[FamilyMemberRelation] = {
    import context.sparkSession.implicits._

    left.joinWith(right, left.col("kfId") === right.col("relativeId"), "left").map(tuple => {

      FamilyMemberRelation(
        tuple._2.participantId,
        tuple._1,
        tuple._2.relativeToParticipantRelation
      )
    })
  }

}
