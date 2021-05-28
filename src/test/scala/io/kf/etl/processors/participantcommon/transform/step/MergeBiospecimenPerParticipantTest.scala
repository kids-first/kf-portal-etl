package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{
  EBiospecimen,
  EBiospecimenDiagnosis,
  EDiagnosis,
  ESequencingCenter
}
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
    val bioSpecimen1 = EBiospecimen(
      kf_id = Some("biospecimen_id_1"),
      participant_id = Some("participant_id_1"),
      ncit_id_anatomical_site = Some("NCIT:unknown"),
      duo_ids = Seq("duoCodeId1", "duoCodeId2", "duoCodeId_NoLabel"),
      sequencing_center_id = Some("SC_CATTVETT")
    )
    val diagnosis = EDiagnosis(kf_id = Some("diagnosis_id_1"))
    val biospecimenDiagnosis = EBiospecimenDiagnosis(
      kf_id = Some("bd1"),
      diagnosis_id = Some("diagnosis_id_1"),
      biospecimen_id = Some("biospecimen_id_1")
    )

    val p2 = Participant_ES(kf_id = Some("participant_id_2"))
    val bioSpecimen21 = EBiospecimen(
      kf_id = Some("biospecimen_id_21"),
      participant_id = Some("participant_id_2"),
      ncit_id_anatomical_site = Some("NCIT:C12438"),
      ncit_id_tissue_type = Some("NCIT:C14165"),
      duo_ids = Seq("duoCodeId21")
    )
    val bioSpecimen22 = EBiospecimen(
      kf_id = Some("biospecimen_id_22"),
      participant_id = Some("participant_id_2")
    )

    val bioSpecimen3 = EBiospecimen(
      kf_id = Some("biospecimen_id_3"),
      participant_id = None
    ) //should be ignore, no participants

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
      duoCodes = Option(duoCode.toDS()),
      sequencingCenters = Seq(
        ESequencingCenter(
          kf_id = Some("SC_CATTVETT"),
          name = Some("Feline Diagnostics LLC")
        )
      )
    )

    val mergeBiospecimen =
      MergeBiospecimenPerParticipant(entityDataset, participantsDataset)
    val result = mergeBiospecimen.collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(
        kf_id = Some("participant_id_1"),
        biospecimens = Seq(
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_1"),
            ncit_id_anatomical_site = Some("NCIT:unknown"),
            diagnoses = Seq(Diagnosis_ES(kf_id = Some("diagnosis_id_1"))),
            duo_code = Seq(
              "Label1 (duoCodeId1)",
              "Label2 (duoCodeId2)",
              "duoCodeId_NoLabel"
            ),
            sequencing_center_id = Some("SC_CATTVETT"),
            sequencing_center = Some("Feline Diagnostics LLC")
          )
        )
      ),
      Participant_ES(
        kf_id = Some("participant_id_2"),
        biospecimens = Seq(
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_21"),
            ncit_id_anatomical_site = Some("Central nervous system (NCIT:C12438)"),
            ncit_id_tissue_type = Some("Normal (NCIT:C14165)"),
            duo_code = Seq("duoCodeId21")
          ),
          Biospecimen_ES(kf_id = Some("biospecimen_id_22"))
        )
      ),
      Participant_ES(kf_id = Some("participant_id_3"))
    )
  }

  "enrichBiospecimenWithDiagnoses" should "join biospecimens with diagnoses" in {
    val bioSpecimens = Seq(
      EBiospecimen(kf_id = Some("biospecimen_id_1")),
      EBiospecimen(kf_id = Some("biospecimen_id_2")),
      EBiospecimen(kf_id = Some("biospecimen_id_3"))
    )

    val diagnoses = Seq(
      EDiagnosis(kf_id = Some("diagnosis_id_1")),
      EDiagnosis(kf_id = Some("diagnosis_id_2")),
      EDiagnosis(kf_id = Some("diagnosis_id_3"))
    )

    val biospecimensDiagnosis = Seq(
      EBiospecimenDiagnosis(
        kf_id = Some("bd1"),
        diagnosis_id = Some("diagnosis_id_1"),
        biospecimen_id = Some("biospecimen_id_1")
      ),
      EBiospecimenDiagnosis(
        kf_id = Some("bd1"),
        diagnosis_id = Some("diagnosis_id_2"),
        biospecimen_id = Some("biospecimen_id_1")
      ),
      EBiospecimenDiagnosis(
        kf_id = Some("bd1"),
        diagnosis_id = Some("diagnosis_id_3"),
        biospecimen_id = Some("biospecimen_id_2")
      )
    )

    val result = MergeBiospecimenPerParticipant
      .enrichBiospecimenWithDiagnoses(
        bioSpecimens.toDS(),
        biospecimensDiagnosis.toDS(),
        diagnoses.toDS()
      )(spark)
      .collect()
    result should contain theSameElementsAs Seq(
      EBiospecimen(
        kf_id = Some("biospecimen_id_1"),
        diagnoses = Seq(
          EDiagnosis(kf_id = Some("diagnosis_id_2")),
          EDiagnosis(kf_id = Some("diagnosis_id_1"))
        )
      ),
      EBiospecimen(
        kf_id = Some("biospecimen_id_2"),
        diagnoses = Seq(EDiagnosis(kf_id = Some("diagnosis_id_3")))
      ),
      EBiospecimen(kf_id = Some("biospecimen_id_3"), diagnoses = Nil)
    )
  }

  "enrichBiospecimenWithSequencingCenters" should "join with sequencing centers" in {
    val bioSpecimens = Seq(
      EBiospecimen(
        kf_id = Some("BS_1QV582PW"),
        sequencing_center_id = Some("SC_5A2B1T4K")
      ),
      EBiospecimen(
        kf_id = Some("BS_14QW6KZQ"),
        sequencing_center_id = Some("SC_A1JNZAZH")
      ),
      EBiospecimen(
        kf_id = Some("BS_FRRRBA11"),
        sequencing_center_id = Some("SC_CATTVETT")
      )
    )

    val sequencingCenters = Seq(
      ESequencingCenter(kf_id = Some("SC_5A2B1T4K"), name = Some("UNKNOWN:CHRIS_JONES")),
      ESequencingCenter(
        kf_id = Some("SC_A1JNZAZH"),
        name = Some("Baylor College of Medicine")
      ),
      ESequencingCenter(kf_id = Some("SC_CATTVETT"), name = Some("Feline Diagnostics LLC"))
    )

    val result = MergeBiospecimenPerParticipant
      .enrichBiospecimenWithSequencingCenters(
        bioSpecimens.toDS(),
        sequencingCenters.toDS()
      )(spark)
      .collect()

    result should contain theSameElementsAs Seq(
      EBiospecimen(
        kf_id = Some("BS_1QV582PW"),
        sequencing_center_id = Some("SC_5A2B1T4K"),
        sequencing_center = Some("UNKNOWN:CHRIS_JONES")
      ),
      EBiospecimen(
        kf_id = Some("BS_14QW6KZQ"),
        sequencing_center_id = Some("SC_A1JNZAZH"),
        sequencing_center = Some("Baylor College of Medicine")
      ),
      EBiospecimen(
        kf_id = Some("BS_FRRRBA11"),
        sequencing_center_id = Some("SC_CATTVETT"),
        sequencing_center = Some("Feline Diagnostics LLC")
      )
    )
  }
}
