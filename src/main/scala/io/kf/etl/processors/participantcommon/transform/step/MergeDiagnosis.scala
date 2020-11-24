package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{EBiospecimenDiagnosis, EDiagnosis}
import io.kf.etl.models.es.{DiagnosisTermWithParents_ES, OntologicalTermWithParents_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import io.kf.etl.processors.common.mergers.OntologyUtil
import org.apache.spark.sql.{Dataset, SparkSession}

object MergeDiagnosis {

  def apply(entityDataset: EntityDataSet, participants: Dataset[Participant_ES])(implicit spark: SparkSession): Dataset[Participant_ES] = {
    import entityDataset.{biospecimenDiagnoses, diagnoses, ontologyData}
    import spark.implicits._
    val diagnosisWithBiospecimens = enrichDiagnosesWithBiospecimens(biospecimenDiagnoses, diagnoses)

    val filteredDiagnosis = diagnosisWithBiospecimens.filter(_.participant_id.isDefined)

    val diagnosisWithBioAndMondo = OntologyUtil.mapDiagnosisTermsToObservable(filteredDiagnosis, "mondo_id_diagnosis")(ontologyData.mondoTerms)

    participants
      .joinWith(
        diagnosisWithBioAndMondo,
        participants.col("kf_id") === diagnosisWithBioAndMondo.col("observable.participant_id"),
        "left_outer"
      )
      .groupByKey { case (participant, _) => participant.kf_id }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq = groups.filter(g => g._2 != null && g._2._1 != null).map{
          case(_, (eDiagnosis, ontologyTerm, ontoTermsWParents)) => {
            val currentOntologicalTerm = if(ontologyTerm != null){
              Seq(DiagnosisTermWithParents_ES(
                name = ontologyTerm.toString,
                parents = ontologyTerm.parents,
                is_leaf = ontologyTerm.is_leaf,
                is_tagged = true
              ))} else Nil

            val currentDiagnosis = currentOntologicalTerm match {
              case Nil => Seq(EntityConverter.EDiagnosisToDiagnosisES(eDiagnosis, None))
              case _ => currentOntologicalTerm.map(p => EntityConverter.EDiagnosisToDiagnosisES(eDiagnosis, Some(p)))
            }
            val parentDiagnosis = ontoTermsWParents.map(p => EntityConverter.EDiagnosisToLightDiagnosisES(eDiagnosis, Some(p)))
            currentDiagnosis ++ parentDiagnosis
          }
        }
        participant.copy(
          diagnoses = filteredSeq.flatten
        )
      })
  }


  def enrichDiagnosesWithBiospecimens(biospecimensDiagnoses: Dataset[EBiospecimenDiagnosis], diagnoses: Dataset[EDiagnosis])(implicit spark: SparkSession): Dataset[EDiagnosis] = {
    import spark.implicits._
    val ds: Dataset[EDiagnosis] = diagnoses.joinWith(biospecimensDiagnoses, diagnoses("kf_id") === biospecimensDiagnoses("diagnosis_id"), joinType = "left")
      .groupByKey(_._1)
      .mapGroups(
        (diagnosis, iter) => diagnosis.copy(biospecimens = iter.collect { case (_, d) if d != null && d.biospecimen_id.isDefined => d.biospecimen_id.get }.toSeq))
    ds
  }
}
