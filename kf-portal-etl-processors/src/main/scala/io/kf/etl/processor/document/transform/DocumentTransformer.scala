package io.kf.etl.processor.document.transform

import io.kf.etl.processor.common.ProcessorCommonDefinitions.{DS_FAMILYRELATIONSHIP, DS_PARTICIPANT, DatasetsFromDBTables}
import io.kf.etl.model._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

class DocumentTransformer(val spark:SparkSession) {

  def transform(input: DatasetsFromDBTables): Dataset[DocType] = {
    ???
  }

  private def buildFamily(all: DatasetsFromDBTables):Unit = {

    val participants = merge_Demographic_Diagnosis_Phenotype_Study_into_Participant(all).cache()

//    participants.groupByKey(p => {
//      p.family match {
//        case Some(family) => family.familyId
//        case None => "null"
//      }
//    }).flatMapGroups((familyId, iterator) => {
//
//      familyId match {
//        case "null" => iterator
//        case _ => {
//          val list = iterator.toList
//
//          list.map(p => {
//
//          })
//
//          list
//        }
//      }
//    })

  }

  private def merge_Demographic_Diagnosis_Phenotype_Study_into_Participant(all: DatasetsFromDBTables): Dataset[Participant] = {
    import spark.implicits._

    all.participant.joinWith(
      all.demographic,
      all.participant.col("kfId") === all.demographic.col("kfId"),
      "left"
    ).map(tuple => {

      Option(tuple._2) match {
        case Some(t_demographic) => {
          Participant(
            kfId = tuple._1.kfId,
            createdAt = tuple._1.createdAt,
            modifiedAt = tuple._1.modifiedAt,
            uuid = tuple._1.uuid,
            family = tuple._1.familyId match {
              case Some(f) => Some(Family(familyId = f))
              case None => None
            },
            isProband = tuple._1.isProband,
            race = t_demographic.race,
            ethnicity = t_demographic.ethnicity,
            gender = t_demographic.gender
          )
        }
        case None => {
          Participant(
            kfId = tuple._1.kfId,
            createdAt = tuple._1.createdAt,
            modifiedAt = tuple._1.modifiedAt,
            uuid = tuple._1.uuid,
            family = tuple._1.familyId match {
              case Some(f) => Some(Family(familyId = f))
              case None => None
            },
            isProband = tuple._1.isProband
          )
        }
      }
    }).joinWith(
      all.diagnosis,
      col("kfId"),
      "left"
    ).groupByKey(tuple => {
      tuple._1.kfId
    }).mapGroups((kfId, iterator) => {

      val list = iterator.toList

      val merged =
        list.foldLeft(new ListBuffer[Diagnosis]){(lb, tuple) => {
          Option(tuple._2) match {
            case Some(t_diagnosis) => {
              lb += Diagnosis(
                kfId = t_diagnosis.kfId,
                createdAt = t_diagnosis.createdAt,
                modifiedAt = t_diagnosis.modifiedAt,
                uuid = t_diagnosis.uuid
                // here more fields should be filled in
              )
            }
            case None => lb
          }
        }}

      merged.size match {
        case 0 => list(0)._1
        case _ => list(0)._1.copy(diagnoses = merged.toList)
      }
    }).joinWith(
      all.phenotype,
      col("kfId"),
      "left"
    ).groupByKey(tuple => {
      tuple._1.kfId
    }).mapGroups((kfId, iterator) => {
      val list = iterator.toList

      val merged = list.foldLeft(new ListBuffer[Phenotype]){(lb, tuple) => {
        Option(tuple._2) match {
          case Some(t_phenotype) => {
            lb += Phenotype(
              // here more fields should be filled in
            )
          }
          case None => lb
        }
      }}

      merged.size match {
        case 0 => list(0)._1
        case _ => list(0)._1.copy(phenotype = merged.toList)
      }
    }).joinWith(
      all.study,
      col("kfId"),
      "left"
    ).groupByKey(tuple => {
      tuple._1.kfId
    }).mapGroups((kfId, iterator) => {
      val list = iterator.toList

      val merged = list.foldLeft(new ListBuffer[Study]){(lb, tuple) => {
        Option(tuple._2) match {
          case Some(t_study) => {
            lb += Study(
              kfId = t_study.kfId,
              uuid = t_study.uuid,
              createdAt = t_study.createdAt,
              modifiedAt = t_study.modifiedAt,
              dataAccessAuthority = t_study.dataAccessAuthority,
              externalId = t_study.externalId,
              version = t_study.version,
              name = t_study.name,
              attribution = t_study.attribution
            )
          }
          case None => lb
        }
      }}

      merged.size match {
        case 0 => list(0)._1
        case _ => list(0)._1.copy(studies = merged.toList)
      }
    })
  }

}
