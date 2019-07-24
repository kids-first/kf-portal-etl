package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Diagnosis_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EDiagnosis
import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.test.util.EntityUtil.{buildEntityDataSet, buildOntologiesDataSet}
import io.kf.etl.processors.test.util.StepContextUtil.buildContext
import io.kf.etl.processors.test.util.{StepContextUtil, WithSparkSession}
import org.scalatest.{FlatSpec, Matchers}

class MergeDiagnosisTest extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  "process" should "merge diagnoses and participnt" in {
    val p1 = Participant_ES(kfId = Some("participant_id_1"))
    val diagnoses11 = EDiagnosis(kfId = Some("diagnosis_11"), participantId = Some("participant_id_1"))
    val diagnoses12 = EDiagnosis(kfId = Some("diagnosis_12"), participantId = Some("participant_id_1"))


    val p2 = Participant_ES(kfId = Some("participant_id_2"))
    val diagnoses21 = EDiagnosis(kfId = Some("diagnosis_21"), participantId = Some("participant_id_2"))

    val diagnoses3 = EDiagnosis(kfId = Some("diagnosis_3"))

    val p3 = Participant_ES(kfId = Some("participant_id_3"))


    val entityDataset = buildEntityDataSet(
      diagnoses = Seq(diagnoses11, diagnoses12, diagnoses21, diagnoses3)
    )

    val merge = new MergeDiagnosis(ctx = buildContext(entityDataset))

    val result = merge.process(Seq(p1, p2, p3).toDS()).collect()

    //We sort diagnoses for each participant
    val sortedResult = result.map(r => r.copy(diagnoses = r.diagnoses.sortBy(_.kfId)))
    sortedResult should contain theSameElementsAs Seq(
      Participant_ES(kfId = Some("participant_id_1"),
        diagnoses = Seq(
          Diagnosis_ES(kfId = Some("diagnosis_11")),
          Diagnosis_ES(kfId = Some("diagnosis_12"))
        )
      ),
      Participant_ES(kfId = Some("participant_id_2"),
        diagnoses = Seq(Diagnosis_ES(kfId = Some("diagnosis_21"))
        )
      ),
      Participant_ES(kfId = Some("participant_id_3"))
    )
  }
}
