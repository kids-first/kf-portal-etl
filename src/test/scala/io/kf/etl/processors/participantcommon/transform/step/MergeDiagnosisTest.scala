package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{EBiospecimenDiagnosis, EDiagnosis}
import io.kf.etl.models.es.{Diagnosis_ES, Participant_ES}
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}

class MergeDiagnosisTest extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  "process" should "merge diagnoses and participant" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val diagnoses11 = EDiagnosis(kfId = Some("diagnosis_11"), participantId = Some("participant_id_1"))
    val diagnoses12 = EDiagnosis(kfId = Some("diagnosis_12"), participantId = Some("participant_id_1"))


    val p2 = Participant_ES(kf_id = Some("participant_id_2"))
    val diagnoses21 = EDiagnosis(kfId = Some("diagnosis_21"), participantId = Some("participant_id_2"))

    val diagnoses3 = EDiagnosis(kfId = Some("diagnosis_3"))

    val p3 = Participant_ES(kf_id = Some("participant_id_3"))

    val biospecimenDiagnosis1 = EBiospecimenDiagnosis(kfId = Some("bd1"), diagnosisId = Some("diagnosis_11"), biospecimenId = Some("biospecimen_id_1"))
    val biospecimenDiagnosis2 = EBiospecimenDiagnosis(kfId = Some("bd1"), diagnosisId = Some("diagnosis_11"), biospecimenId = Some("biospecimen_id_2"))


    val entityDataset = buildEntityDataSet(
      diagnoses = Seq(diagnoses11, diagnoses12, diagnoses21, diagnoses3),
      biospecimenDiagnoses = Seq(biospecimenDiagnosis1, biospecimenDiagnosis2)

    )


    val result = MergeDiagnosis(entityDataset, Seq(p1, p2, p3).toDS()).collect()

    //We sort diagnoses for each participant
    val sortedResult = result.map(r => r.copy(diagnoses = r.diagnoses.sortBy(_.kf_id)))
    sortedResult should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("participant_id_1"),
        diagnoses = Seq(
          Diagnosis_ES(kf_id = Some("diagnosis_11"), biospecimens = Seq("biospecimen_id_2","biospecimen_id_1")),
          Diagnosis_ES(kf_id = Some("diagnosis_12"))
        )
      ),
      Participant_ES(kf_id = Some("participant_id_2"),
        diagnoses = Seq(Diagnosis_ES(kf_id = Some("diagnosis_21"))
        )
      ),
      Participant_ES(kf_id = Some("participant_id_3"))
    )
  }
}
