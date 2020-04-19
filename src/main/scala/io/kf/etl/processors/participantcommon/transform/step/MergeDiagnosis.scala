package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{EBiospecimenDiagnosis, EDiagnosis}
import io.kf.etl.models.es.{OntologicalTermWithParents_ES, Participant_ES}
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

    val diagnosisWithBioAndMondo = OntologyUtil.mapOntologyTermsToObservable(filteredDiagnosis, "mondo_id_diagnosis")(ontologyData.mondoTerms)

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
            val diagnosis_ES = EntityConverter.EDiagnosisToDiagnosisES(eDiagnosis)
            val currentOntologicalTerm = if(ontologyTerm != null){
              Seq(OntologicalTermWithParents_ES(
                name = ontologyTerm.toString,
                parents = ontologyTerm.parents,
                age_at_event_days = if(eDiagnosis.age_at_event_days.isDefined) Set(eDiagnosis.age_at_event_days.get) else Set.empty[Int],
                is_leaf = ontologyTerm.is_leaf
              ))} else Nil
            val mergedOntoTermsWParents = currentOntologicalTerm ++ ontoTermsWParents
            diagnosis_ES -> mergedOntoTermsWParents
          }
        }
        participant.copy(
          diagnoses = filteredSeq.map(_._1),
          mondo_diagnosis = OntologyUtil.groupOntologyTermsWithParents(filteredSeq.flatMap(_._2))
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
