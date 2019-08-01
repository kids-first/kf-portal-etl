package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Outcome_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EOutcome
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.StepContextUtil.buildContext
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}

class MergeOutcomeTest extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  "process" should "merge outcomes and participant" in {
    val p1 = Participant_ES(kfId = Some("participant_id_1"))
    val outcome11 = EOutcome(kfId = Some("outcome_11"), participantId = Some("participant_id_1"), vitalStatus = Some("Alive"), ageAtEventDays = Some(100))
    val outcome12 = EOutcome(kfId = Some("outcome_12"), participantId = Some("participant_id_1"), vitalStatus = Some("Deceased"), ageAtEventDays = Some(200))
    val outcome13 = EOutcome(kfId = Some("outcome_13"), participantId = Some("participant_id_1"), vitalStatus = Some("Alive"), ageAtEventDays = Some(200))


    val p2 = Participant_ES(kfId = Some("participant_id_2"))
    val outcome21 = EOutcome(kfId = Some("outcome_21"), participantId = Some("participant_id_2"), vitalStatus = Some("Alive"), diseaseRelated = Some("NA"), ageAtEventDays = Some(50))
//    val outcome22 = EOutcome(kfId = Some("outcome_22"), participantId = Some("participant_id_2"), vitalStatus = Some("Deceased"), diseaseRelated = Some("NA"), ageAtEventDays = None)
    val p3 = Participant_ES(kfId = Some("participant_id_3"))

    val entityDataset = buildEntityDataSet(
      outcomes = Seq(outcome11, outcome12, outcome13, outcome21)
    )

    val merge = new MergeOutcome(ctx = buildContext(entityDataset))

    val result = merge.process(Seq(p1, p2, p3).toDS()).collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(kfId = Some("participant_id_1"),
        outcome = Some(Outcome_ES(ageAtEventDays = Some(200), kfId = Some("outcome_12"), vitalStatus = Some("Deceased")))
      ),
      Participant_ES(kfId = Some("participant_id_2"),
        outcome = Some(Outcome_ES(ageAtEventDays = Some(50), kfId = Some("outcome_21"), vitalStatus = Some("Alive"), diseaseRelated = Some("NA")))
      ),

      Participant_ES(kfId = Some("participant_id_3"))
    )
  }


}
