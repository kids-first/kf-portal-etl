package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{EBiospecimenDiagnosis, EDiagnosis}
import io.kf.etl.models.es.{DiagnosisTermWithParents_ES, Diagnosis_ES, OntologicalTermWithParents_ES, Participant_ES}
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
    mondoTerms = spark.read.json("../kf-portal-etl/kf-portal-etl-docker/mondo_terms.json.gz").select("id", "name", "parents", "ancestors", "is_leaf").as[OntologyTerm],
    ncitTerms = Seq.empty[OntologyTermBasic].toDS()
  )

  "process" should "merge diagnoses and participant" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val diagnoses11 = EDiagnosis(kf_id = Some("diagnosis_11"), participant_id = Some("participant_id_1"))
    val diagnoses12 = EDiagnosis(kf_id = Some("diagnosis_12"), participant_id = Some("participant_id_1"))


    val p2 = Participant_ES(kf_id = Some("participant_id_2"))
    val diagnoses21 = EDiagnosis(
      kf_id = Some("diagnosis_21"),
      participant_id = Some("participant_id_2"),
      mondo_id_diagnosis = Some("MONDO:0043197"),
      diagnosis_text = Some("ruvalcaba churesigaew myhre syndrome"),
      age_at_event_days = Some(15),
      diagnosis_category = Some("awesome")
    )
    val diagnoses22 = EDiagnosis(kf_id = Some("diagnosis_22"), participant_id = Some("participant_id_2"), mondo_id_diagnosis = Some("MONDO:0000232"), age_at_event_days = Some(18))

    val diagnoses3 = EDiagnosis(kf_id = Some("diagnosis_3"))

    val p3 = Participant_ES(kf_id = Some("participant_id_3"))

    val biospecimenDiagnosis1 = EBiospecimenDiagnosis(kf_id = Some("bd1"), diagnosis_id = Some("diagnosis_11"), biospecimen_id = Some("biospecimen_id_1"))
    val biospecimenDiagnosis2 = EBiospecimenDiagnosis(kf_id = Some("bd2"), diagnosis_id = Some("diagnosis_11"), biospecimen_id = Some("biospecimen_id_2"))
    val biospecimenDiagnosis3 = EBiospecimenDiagnosis(kf_id = Some("bd3"), diagnosis_id = Some("diagnosis_22"), biospecimen_id = Some("biospecimen_id_3"))


    val entityDataset = buildEntityDataSet(
      diagnoses = Seq(diagnoses11, diagnoses12, diagnoses21, diagnoses22, diagnoses3),
      biospecimenDiagnoses = Seq(biospecimenDiagnosis1, biospecimenDiagnosis2, biospecimenDiagnosis3),
      ontologyData = Some(ontologiesDataSet)
    )


    val result = MergeDiagnosis(entityDataset, Seq(p1, p2, p3).toDS()).collect()

    val resultP1 = result.find(_.kf_id.getOrElse("") == "participant_id_1")
    val resultP2 = result.find(_.kf_id.getOrElse("") == "participant_id_2")
    val resultP3 = result.find(_.kf_id.getOrElse("") == "participant_id_3")

    resultP1 match {
      case Some(p) => p.diagnoses should contain theSameElementsAs Seq(
        Diagnosis_ES(kf_id = Some("diagnosis_11"), biospecimens = Seq("biospecimen_id_2","biospecimen_id_1")),
        Diagnosis_ES(kf_id = Some("diagnosis_12"))
      )
      case None => fail("kf_id: participant_id_1 was not found")
    }

    resultP2 match {
      case Some(p) => p.diagnoses should contain theSameElementsAs Seq(
        Diagnosis_ES(
          kf_id = Some("diagnosis_21"),
          mondo_id_diagnosis = Some(mondo_0043197.toString),
          diagnosis = Some(mondo_0043197.name),
          diagnosis_category = Some("awesome"),
          is_tagged = true,
          age_at_event_days = Some(15),
          mondo = Seq(DiagnosisTermWithParents_ES(
            name = mondo_0043197.toString,
            parents = Seq(mondo_0002254.toString),
            is_leaf = true,
            is_tagged = true
          ))),
        Diagnosis_ES(diagnosis_category = Some("awesome"), age_at_event_days = Some(15), mondo = Seq(DiagnosisTermWithParents_ES(name = mondo_0002254.toString, parents = Seq(mondo_0000001.toString)))),
        Diagnosis_ES(diagnosis_category = Some("awesome") ,age_at_event_days = Some(15), mondo = Seq(DiagnosisTermWithParents_ES(name = mondo_0000001.toString, parents = Seq.empty[String]))),
        Diagnosis_ES(
          kf_id = Some("diagnosis_22"),
          mondo_id_diagnosis = Some(mondo_0000232.id),
          biospecimens = Seq("biospecimen_id_3"),
          age_at_event_days = Some(18),
          is_tagged = true,
          mondo = Seq(DiagnosisTermWithParents_ES(
            name = mondo_0000232.toString,
            parents = Seq(mondo_0001195.toString),
            is_leaf = true,
            is_tagged = true
          ))),
        Diagnosis_ES(age_at_event_days = Some(18), mondo = Seq(DiagnosisTermWithParents_ES(name = mondo_0001195.toString, parents = Seq(mondo_0006927.toString)))),
        Diagnosis_ES(age_at_event_days = Some(18), mondo = Seq(DiagnosisTermWithParents_ES(name = mondo_0006927.toString, parents = Seq(mondo_0006956.toString, mondo_0021678.toString)))),
        Diagnosis_ES(age_at_event_days = Some(18), mondo = Seq(DiagnosisTermWithParents_ES(name = mondo_0006956.toString, parents = Seq(mondo_0005113.toString)))),
        Diagnosis_ES(age_at_event_days = Some(18), mondo = Seq(DiagnosisTermWithParents_ES(name = mondo_0021678.toString, parents = Seq(mondo_0005113.toString)))),
        Diagnosis_ES(age_at_event_days = Some(18), mondo = Seq(DiagnosisTermWithParents_ES(name = mondo_0005113.toString, parents = Seq(mondo_0005550.toString)))),
        Diagnosis_ES(age_at_event_days = Some(18), mondo = Seq(DiagnosisTermWithParents_ES(name = mondo_0005550.toString, parents = Seq(mondo_0000001.toString)))),
        Diagnosis_ES(age_at_event_days = Some(18), mondo = Seq(DiagnosisTermWithParents_ES(name = mondo_0000001.toString, parents = Seq.empty[String])))
      )
      case None => fail("kf_id: participant_id_2 was not found")
    }

    resultP3 match {
      case Some(p) => p.diagnoses should contain theSameElementsAs Seq.empty[Diagnosis_ES]
      case None => fail("kf_id: participant_id_3 was not found")
    }
  }
}
