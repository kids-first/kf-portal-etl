package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{
  EBiospecimen,
  EBiospecimenDiagnosis,
  EDiagnosis,
  ESequencingCenter
}
import io.kf.etl.models.duocode.DuoCode
import io.kf.etl.models.es.{Biospecimen_ES, Participant_ES}
import io.kf.etl.models.ontology.OntologyTermBasic
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object MergeBiospecimenPerParticipant {
  def apply(
      entityDataset: EntityDataSet,
      participants: Dataset[Participant_ES]
  )(implicit spark: SparkSession): Dataset[Participant_ES] = {
    import entityDataset.ontologyData.ncitTerms
    import entityDataset.{
      biospecimenDiagnoses,
      biospecimens,
      diagnoses,
      duoCodeDataSet,
      sequencingCenters
    }
    import spark.implicits._

    val biospecimenJoinedWithNCIT: Dataset[EBiospecimen] =
      enrichBiospecimenWithNcitTerms(biospecimens, ncitTerms)
    val biospecimenJoinedWithDiagnosis = enrichBiospecimenWithDiagnoses(
      biospecimenJoinedWithNCIT,
      biospecimenDiagnoses,
      diagnoses
    )
    val biospecimenJoinedDuoCode =
      enrichBiospecimenDuoCode(biospecimenJoinedWithDiagnosis, duoCodeDataSet)

    val biospecimenWithSequencingCenters = {
      enrichBiospecimenWithSequencingCenters(
        biospecimenJoinedDuoCode,
        sequencingCenters
      )
    }

    participants
      .joinWith(
        biospecimenWithSequencingCenters,
        participants.col("kf_id") === biospecimenWithSequencingCenters.col(
          "participant_id"
        ),
        "left_outer"
      )
      .groupByKey { case (participant, _) => participant.kf_id }
      .mapGroups((_, groupsIterator) => {
        val groups      = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq: Seq[Biospecimen_ES] = groups.collect {
          case (_, b) if b != null =>
            EntityConverter.EBiospecimenToBiospecimenES(b)
        }
        participant.copy(
          biospecimens = filteredSeq
        )
      })
  }

  private def formatTerm(term: OntologyTermBasic): Option[String] =
    Some(s"${term.name} (${term.id})")

  private def enrichBiospecimenWithNcitTerms(
      biospecimens: Dataset[EBiospecimen],
      ncitTerms: Dataset[OntologyTermBasic]
  )(implicit spark: SparkSession) = {
    import spark.implicits._
    biospecimens
      .joinWith(
        ncitTerms,
        biospecimens.col("ncit_id_anatomical_site") === ncitTerms.col("id"),
        "left"
      )
      .map {
        case (biospeciem, term) if term != null =>
          biospeciem.copy(ncit_id_anatomical_site = formatTerm(term))
        case (biospecimen, _) => biospecimen
      }
      .joinWith(
        ncitTerms,
        $"ncit_id_tissue_type" === ncitTerms.col("id"),
        "left"
      )
      .map {
        case (biospeciem, term) if term != null =>
          biospeciem.copy(ncit_id_tissue_type = formatTerm(term))
        case (biospeciem, _) => biospeciem
      }
  }

  def enrichBiospecimenWithDiagnoses(
      biospecimens: Dataset[EBiospecimen],
      biospecimensDiagnoses: Dataset[EBiospecimenDiagnosis],
      diagnoses: Dataset[EDiagnosis]
  )(implicit spark: SparkSession): Dataset[EBiospecimen] = {
    import spark.implicits._
    val ds: Dataset[EBiospecimen] = biospecimens
      .joinWith(
        biospecimensDiagnoses,
        biospecimens("kf_id") === biospecimensDiagnoses("biospecimen_id"),
        joinType = "left"
      )
      .joinWith(
        diagnoses,
        diagnoses("kf_id") === $"_2.diagnosis_id",
        joinType = "left"
      )
      .map { case ((b, _), d) => (b, d) }
      .groupByKey(_._1)
      .mapGroups((biospecimen, iter) =>
        biospecimen.copy(diagnoses = iter.collect {
          case (_, d) if d != null => d
        }.toSeq)
      )
    ds
  }

  def enrichBiospecimenDuoCode(
      biospecimens: Dataset[EBiospecimen],
      duoCodeDataSet: Dataset[DuoCode]
  )(implicit spark: SparkSession): Dataset[EBiospecimen] = {
    import org.apache.spark.sql.functions.{collect_list, explode_outer, first, when}
    import spark.implicits._

    val bioId_duoId = biospecimens
      .withColumn("duo_id", explode_outer($"duo_ids"))

    val formattedDuocodes = duoCodeDataSet
      .map(d => (d.id, d.toString))
      .withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "label")

    val b = bioId_duoId
      .joinWith(
        formattedDuocodes,
        $"duo_id" === formattedDuocodes("id"),
        "left_outer"
      )
      .withColumnRenamed("_1", "biospecimen")
      .withColumn(
        "duocode",
        when($"_2".isNotNull, $"_2.label").otherwise($"biospecimen.duo_id")
      )
      .drop("_2")
      .as[(EBiospecimen, String)]

    val df = b
      .groupBy(
        "biospecimen.kf_id"
      )
      .agg(
        first("biospecimen") as "biospecimen",
        collect_list("duocode") as "duocodes"
      )
      .select("biospecimen", "duocodes")
      .as[(EBiospecimen, Seq[String])]

    df.map {
      case (biospecimen, Nil) => biospecimen
      case (biospecimen, duocodes) =>
        biospecimen.copy(duo_ids = duocodes)
    }
  }

  def enrichBiospecimenWithSequencingCenters(
      biospecimens: Dataset[EBiospecimen],
      sequencingCenters: Dataset[ESequencingCenter]
  )(implicit spark: SparkSession): Dataset[EBiospecimen] = {
    import spark.implicits._

    biospecimens
      .joinWith(
        sequencingCenters,
        biospecimens.col("sequencing_center_id") === sequencingCenters.col(
          "kf_id"
        ),
        "left"
      )
      .map {
        case (biospecimen, sequencingCenter)
            if sequencingCenter != null && sequencingCenter.name.isDefined =>
          biospecimen.copy(sequencing_center = sequencingCenter.name)
        case (biospecimen, _) => biospecimen
      }
  }

}
