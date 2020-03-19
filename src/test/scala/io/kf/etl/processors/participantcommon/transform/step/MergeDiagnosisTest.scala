package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{EBiospecimenDiagnosis, EDiagnosis}
import io.kf.etl.models.es.{Diagnosis_ES, OntologicalTermWithParents_ES, Participant_ES}
import io.kf.etl.models.ontology.{OntologyTerm, OntologyTermBasic}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.OntologiesDataSet
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}

class MergeDiagnosisTest extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  val mondo_0043197: OntologyTermBasic = OntologyTermBasic("MONDO:0043197", "ruvalcaba churesigaew myhre syndrome")
  val mondo_0002254: OntologyTermBasic = OntologyTermBasic("MONDO:0002254", "syndromic disease")
  val mondo_0000001: OntologyTermBasic = OntologyTermBasic("MONDO:0000001", "disease or disorder")
  val mondo_0000232: OntologyTermBasic = OntologyTermBasic("MONDO:0000232", "Flinders island spotted fever")
  val mondo_0001195: OntologyTermBasic = OntologyTermBasic("MONDO:0001195", "spotted fever")
  val mondo_0021678: OntologyTermBasic = OntologyTermBasic("MONDO:0021678", "gram-negative bacterial infections")
  val mondo_0005113: OntologyTermBasic = OntologyTermBasic("MONDO:0005113", "bacterial infectious disease")
  val mondo_0005550: OntologyTermBasic = OntologyTermBasic("MONDO:0005550", "infectious disease")
  val mondo_0006956: OntologyTermBasic = OntologyTermBasic("MONDO:0006956", "Rickettsiosis")
  val mondo_0006927: OntologyTermBasic = OntologyTermBasic("MONDO:0006927", "Rickettsiaceae infectious disease")


  val ontologiesDataSet: OntologiesDataSet = OntologiesDataSet(
    hpoTerms = Seq.empty[OntologyTerm].toDS(),
    mondoTerms = spark.read.json("../kf-portal-etl/kf-portal-etl-docker/mondo_terms.json.gz").select("id", "name", "parents", "ancestors", "isLeaf").as[OntologyTerm],
    ncitTerms = Seq.empty[OntologyTermBasic].toDS()
  )

  "process" should "merge diagnoses and participant" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val diagnoses11 = EDiagnosis(kfId = Some("diagnosis_11"), participantId = Some("participant_id_1"))
    val diagnoses12 = EDiagnosis(kfId = Some("diagnosis_12"), participantId = Some("participant_id_1"))


    val p2 = Participant_ES(kf_id = Some("participant_id_2"))
    val diagnoses21 = EDiagnosis(kfId = Some("diagnosis_21"), participantId = Some("participant_id_2"), mondoIdDiagnosis = Some("MONDO:0043197"),diagnosisText = Some("ruvalcaba churesigaew myhre syndrome"), ageAtEventDays = Some(15))
    val diagnoses22 = EDiagnosis(kfId = Some("diagnosis_22"), participantId = Some("participant_id_2"), mondoIdDiagnosis = Some("MONDO:0000232"), ageAtEventDays = Some(18))

    val diagnoses3 = EDiagnosis(kfId = Some("diagnosis_3"))

    val p3 = Participant_ES(kf_id = Some("participant_id_3"))

    val biospecimenDiagnosis1 = EBiospecimenDiagnosis(kfId = Some("bd1"), diagnosisId = Some("diagnosis_11"), biospecimenId = Some("biospecimen_id_1"))
    val biospecimenDiagnosis2 = EBiospecimenDiagnosis(kfId = Some("bd2"), diagnosisId = Some("diagnosis_11"), biospecimenId = Some("biospecimen_id_2"))
    val biospecimenDiagnosis3 = EBiospecimenDiagnosis(kfId = Some("bd3"), diagnosisId = Some("diagnosis_22"), biospecimenId = Some("biospecimen_id_3"))


    val entityDataset = buildEntityDataSet(
      diagnoses = Seq(diagnoses11, diagnoses12, diagnoses21, diagnoses22, diagnoses3),
      biospecimenDiagnoses = Seq(biospecimenDiagnosis1, biospecimenDiagnosis2, biospecimenDiagnosis3),
      ontologyData = Some(ontologiesDataSet)
    )


    val result = MergeDiagnosis(entityDataset, Seq(p1, p2, p3).toDS()).collect()

    //We sort diagnoses for each participant
    val sortedResult = result.map(r => r.copy(diagnoses = r.diagnoses.sortBy(_.kf_id), mondo_diagnosis = r.mondo_diagnosis.sorted))

    sortedResult should contain theSameElementsAs Seq(
      Participant_ES(
        kf_id = Some("participant_id_1"),
        diagnoses = Seq(
          Diagnosis_ES(kf_id = Some("diagnosis_11"), biospecimens = Seq("biospecimen_id_2","biospecimen_id_1")),
          Diagnosis_ES(kf_id = Some("diagnosis_12"))
        )
      ),
      Participant_ES(kf_id = Some("participant_id_2"),
        diagnoses = Seq(
          Diagnosis_ES(kf_id = Some("diagnosis_21"), age_at_event_days = Some(15), mondo_id_diagnosis = Some(mondo_0043197.toString), diagnosis = Some(mondo_0043197.name)),
          Diagnosis_ES(kf_id = Some("diagnosis_22"), age_at_event_days = Some(18), mondo_id_diagnosis = Some(mondo_0000232.id), biospecimens = Seq("biospecimen_id_3"))
        ),
        mondo_diagnosis = Seq(
          OntologicalTermWithParents_ES(name = mondo_0006956.toString, parents = Seq(mondo_0005113.toString), age_at_event_days = Set(18)),
          OntologicalTermWithParents_ES(name = mondo_0005113.toString, parents = Seq(mondo_0005550.toString), age_at_event_days = Set(18)),
          OntologicalTermWithParents_ES(name = mondo_0002254.toString, parents = Seq(mondo_0000001.toString), age_at_event_days = Set(15)),
          OntologicalTermWithParents_ES(name = mondo_0001195.toString, parents = Seq(mondo_0006927.toString), age_at_event_days = Set(18)),
          OntologicalTermWithParents_ES(name = mondo_0006927.toString, parents = Seq(mondo_0006956.toString, mondo_0021678.toString), age_at_event_days = Set(18)),
          OntologicalTermWithParents_ES(name = mondo_0005550.toString, parents = Seq(mondo_0000001.toString), age_at_event_days = Set(18)),
          OntologicalTermWithParents_ES(name = mondo_0021678.toString, parents = Seq(mondo_0005113.toString), age_at_event_days = Set(18)),
          OntologicalTermWithParents_ES(name = mondo_0043197.toString, parents = Seq(mondo_0002254.toString), age_at_event_days = Set(15), isLeaf = true),
          OntologicalTermWithParents_ES(name = mondo_0000232.toString, parents = Seq(mondo_0001195.toString), age_at_event_days = Set(18), isLeaf = true),
          OntologicalTermWithParents_ES(name = mondo_0000001.toString, parents = Seq.empty[String], age_at_event_days = Set(15, 18))
        ).sorted
      ),
      Participant_ES(kf_id = Some("participant_id_3"))
    )
  }
}
