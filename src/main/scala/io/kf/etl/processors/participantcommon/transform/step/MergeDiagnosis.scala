package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{EBiospecimenDiagnosis, EDiagnosis}
import io.kf.etl.models.es.{Diagnosis_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, OntologiesDataSet}
import io.kf.etl.processors.common.converter.EntityConverter
import io.kf.etl.processors.common.mergers.MergersTool
import org.apache.spark.sql.{Dataset, SparkSession}

object MergeDiagnosis {

  def apply(entityDataset: EntityDataSet, participants: Dataset[Participant_ES])(implicit spark: SparkSession): Dataset[Participant_ES] = {
    import entityDataset.{biospecimenDiagnoses, diagnoses, ontologyData}
    import spark.implicits._
    val diagnosisWithBiospecimens = enrichDiagnosesWithBiospecimens(biospecimenDiagnoses, diagnoses)

    enrichParticipantsWithMondoDiagnosis(diagnosisWithBiospecimens, ontologyData)

    participants
      .joinWith(
        diagnosisWithBiospecimens,
        participants.col("kf_id") === diagnosisWithBiospecimens.col("participantId"),
        "left_outer"
      )
      .as[(Participant_ES, Option[EDiagnosis])]
      .groupByKey { case (participant, _) => participant.kf_id }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq: Seq[Diagnosis_ES] = groups.collect { case (_, Some(d)) => EntityConverter.EDiagnosisToDiagnosisES(d) }
        participant.copy(
          diagnoses = filteredSeq
        )
      })
  }


  def enrichDiagnosesWithBiospecimens(biospecimensDiagnoses: Dataset[EBiospecimenDiagnosis], diagnoses: Dataset[EDiagnosis])(implicit spark: SparkSession): Dataset[EDiagnosis] = {
    import spark.implicits._
    val ds: Dataset[EDiagnosis] = diagnoses.joinWith(biospecimensDiagnoses, diagnoses("kfId") === biospecimensDiagnoses("diagnosisId"), joinType = "left")
      .groupByKey(_._1)
      .mapGroups(
        (diagnosis, iter) => diagnosis.copy(biospecimens = iter.collect { case (_, d) if d != null && d.biospecimenId.isDefined => d.biospecimenId.get }.toSeq))
    ds
  }

  def enrichParticipantsWithMondoDiagnosis(diagnosisWithBiospecimens: Dataset[EDiagnosis], ontologicalData: OntologiesDataSet)(implicit spark: SparkSession) = { //FIXME fix name
    import ontologicalData.mondoTerms
    import spark.implicits._

    val filteredDiagnosis = diagnosisWithBiospecimens.filter(_.participantId.isDefined)

    val test = MergersTool.mapOntologyTermsToObservable(filteredDiagnosis, "mondoIdDiagnosis")(mondoTerms)

  }

}
