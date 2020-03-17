package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{EBiospecimen, EBiospecimenDiagnosis, EDiagnosis}
import io.kf.etl.models.duocode.DuoCode
import io.kf.etl.models.es.{Biospecimen_ES, Diagnosis_ES, Participant_ES}
import io.kf.etl.models.ontology.OntologyTermBasic
import io.kf.etl.processors.test.util.EntityUtil._
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}

class MergeBiospecimenPerParticipantTest extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  val duoCode = Seq(
    DuoCode(id = "duoCodeId1", label = Some("Label1")),
    DuoCode(id = "duoCodeId2", label = Some("Label2")),
    DuoCode(id = "duoCodeId3", label = Some("Label3"))
  )

  "process" should "join biospecimen and participant and enrich the ncid tissue type, anatomical site and diagnoses" in {

    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val bioSpecimen1 = EBiospecimen(kfId = Some("biospecimen_id_1"), participantId = Some("participant_id_1"), ncitIdAnatomicalSite = Some("NCIT:unknown"), duoIds = Seq("duoCodeId1", "duoCodeId2", "duoCodeId_NoLabel"))
    val diagnosis = EDiagnosis(kfId = Some("diagnosis_id_1"))
    val biospecimenDiagnosis = EBiospecimenDiagnosis(kfId = Some("bd1"), diagnosisId = Some("diagnosis_id_1"), biospecimenId = Some("biospecimen_id_1"))

    val p2 = Participant_ES(kf_id = Some("participant_id_2"))
    val bioSpecimen21 = EBiospecimen(kfId = Some("biospecimen_id_21"), participantId = Some("participant_id_2"), ncitIdAnatomicalSite = Some("NCIT:C12438"), ncitIdTissueType = Some("NCIT:C14165"), duoIds = Seq("duoCodeId21"))
    val bioSpecimen22 = EBiospecimen(kfId = Some("biospecimen_id_22"), participantId = Some("participant_id_2"))

    val bioSpecimen3 = EBiospecimen(kfId = Some("biospecimen_id_3"), participantId = None) //should be ignore, no participants

    val p3 = Participant_ES(kf_id = Some("participant_id_3"))

    val participantsDataset = Seq(p1, p2, p3).toDS()

    val ontologiesDataset = buildOntologiesDataSet(
      ncitTerms = Seq(
        OntologyTermBasic(name = "Central nervous system", id = "NCIT:C12438"),
        OntologyTermBasic(name = "Normal", id = "NCIT:C14165")
      )
    )

    val entityDataset = buildEntityDataSet(
      biospecimens = Seq(bioSpecimen1, bioSpecimen21, bioSpecimen22, bioSpecimen3),
      ontologyData = Some(ontologiesDataset),
      diagnoses = Seq(diagnosis),
      biospecimenDiagnoses = Seq(biospecimenDiagnosis),
      duoCodes = Option(duoCode.toDS())
    )

    val mergeBiospecimen = MergeBiospecimenPerParticipant(entityDataset, participantsDataset)
    val result = mergeBiospecimen.collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("participant_id_1"),
        biospecimens = Seq(Biospecimen_ES(kf_id = Some("biospecimen_id_1"), ncit_id_anatomical_site = Some("NCIT:unknown"), diagnoses = Seq(Diagnosis_ES(kf_id = Some("diagnosis_id_1"))), duo_code = Seq("Label1 (duoCodeId1)", "Label2 (duoCodeId2)", "duoCodeId_NoLabel")))
      ),
      Participant_ES(kf_id = Some("participant_id_2"),
        biospecimens = Seq(
          Biospecimen_ES(kf_id = Some("biospecimen_id_21"), ncit_id_anatomical_site = Some("Central nervous system (NCIT:C12438)"), ncit_id_tissue_type = Some("Normal (NCIT:C14165)"), duo_code = Seq("duoCodeId21")),
          Biospecimen_ES(kf_id = Some("biospecimen_id_22"))
        )),
      Participant_ES(kf_id = Some("participant_id_3"))
    )
  }

  "enrichBiospecimenWithDiagnoses" should "join biospecimens with diagnoses" in {
    val bioSpecimens = Seq(
      EBiospecimen(kfId = Some("biospecimen_id_1")),
      EBiospecimen(kfId = Some("biospecimen_id_2")),
      EBiospecimen(kfId = Some("biospecimen_id_3"))

    )

    val diagnoses = Seq(
      EDiagnosis(kfId = Some("diagnosis_id_1")),
      EDiagnosis(kfId = Some("diagnosis_id_2")),
      EDiagnosis(kfId = Some("diagnosis_id_3"))
    )

    val biospecimensDiagnosis = Seq(
      EBiospecimenDiagnosis(kfId = Some("bd1"), diagnosisId = Some("diagnosis_id_1"), biospecimenId = Some("biospecimen_id_1")),
      EBiospecimenDiagnosis(kfId = Some("bd1"), diagnosisId = Some("diagnosis_id_2"), biospecimenId = Some("biospecimen_id_1")),
      EBiospecimenDiagnosis(kfId = Some("bd1"), diagnosisId = Some("diagnosis_id_3"), biospecimenId = Some("biospecimen_id_2"))
    )

    val result = MergeBiospecimenPerParticipant.enrichBiospecimenWithDiagnoses(bioSpecimens.toDS(), biospecimensDiagnosis.toDS(), diagnoses.toDS())(spark).collect()
    result should contain theSameElementsAs Seq(
      EBiospecimen(kfId = Some("biospecimen_id_1"), diagnoses = Seq(EDiagnosis(kfId = Some("diagnosis_id_2")), EDiagnosis(kfId = Some("diagnosis_id_1")))),
      EBiospecimen(kfId = Some("biospecimen_id_2"), diagnoses = Seq(EDiagnosis(kfId = Some("diagnosis_id_3")))),
      EBiospecimen(kfId = Some("biospecimen_id_3"), diagnoses = Nil)
    )
  }
}
