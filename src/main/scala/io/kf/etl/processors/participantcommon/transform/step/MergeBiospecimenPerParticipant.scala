package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{EBiospecimen, EBiospecimenDiagnosis, EDiagnosis}
import io.kf.etl.models.es.{Biospecimen_ES, Participant_ES}
import io.kf.etl.models.ontology.OntologyTerm
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object MergeBiospecimenPerParticipant {
  def apply(entityDataset: EntityDataSet, participants: Dataset[Participant_ES])(implicit spark: SparkSession): Dataset[Participant_ES] = {
    import entityDataset.ontologyData.ncitTerms
    import entityDataset.{biospecimenDiagnoses, biospecimens, diagnoses}
    import spark.implicits._

    val biospecimenJoinedWithNCIT: Dataset[EBiospecimen] = enrichBiospecimenWithNcitTerms(biospecimens, ncitTerms)
    val biospecimenJoinedWithDiagnosis = enrichBiospecimenWithDiagnoses(biospecimenJoinedWithNCIT, biospecimenDiagnoses, diagnoses)

    participants.joinWith(
      biospecimenJoinedWithDiagnosis,
      participants.col("kf_id") === biospecimenJoinedWithDiagnosis.col("participantId"),
      "left_outer"
    ).groupByKey { case (participant, _) => participant.kf_id }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq: Seq[Biospecimen_ES] = groups.collect { case (_, b) if b != null => EntityConverter.EBiospecimenToBiospecimenES(b) }
        participant.copy(
          biospecimens = filteredSeq
        )
      })
  }

  def formatTerm(term: OntologyTerm) = Some(s"${term.name} (${term.id})")

  private def enrichBiospecimenWithNcitTerms(biospecimens: Dataset[EBiospecimen], ncitTerms: Dataset[OntologyTerm])(implicit spark: SparkSession) = {
    import spark.implicits._
    biospecimens
      .joinWith(ncitTerms, biospecimens.col("ncitIdAnatomicalSite") === ncitTerms.col("id"), "left")
      .map {
        case (biospeciem, term) if term != null => biospeciem.copy(ncitIdAnatomicalSite = formatTerm(term))
        case (biospecimen, _) => biospecimen
      }
      .joinWith(ncitTerms, $"ncitIdTissueType" === ncitTerms.col("id"), "left")
      .map {
        case (biospeciem, term) if term != null => biospeciem.copy(ncitIdTissueType = formatTerm(term))
        case (biospeciem, _) => biospeciem
      }
  }

  def enrichBiospecimenWithDiagnoses(biospecimens: Dataset[EBiospecimen], biospecimensDiagnoses: Dataset[EBiospecimenDiagnosis], diagnoses: Dataset[EDiagnosis])(implicit spark: SparkSession): Dataset[EBiospecimen] = {
    import spark.implicits._
    val ds: Dataset[EBiospecimen] = biospecimens.joinWith(biospecimensDiagnoses, biospecimens("kfId") === biospecimensDiagnoses("biospecimenId"), joinType = "left")
      .joinWith(diagnoses, diagnoses("kfId") === $"_2.diagnosisId", joinType = "left")
      .map { case ((b, _), d) => (b, d) }
      .groupByKey(_._1)
      .mapGroups(
        (biospecimen, iter) => biospecimen.copy(diagnoses = iter.collect { case (_, d) if d != null => d }.toSeq))
    ds
  }

}
