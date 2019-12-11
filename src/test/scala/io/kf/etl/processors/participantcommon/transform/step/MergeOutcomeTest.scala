package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.EOutcome
import io.kf.etl.models.es.{Outcome_ES, Participant_ES}
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}

class MergeOutcomeTest extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  "process" should "merge outcomes and participant" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val outcome11 = EOutcome(kfId = Some("outcome_11"), participantId = Some("participant_id_1"), vitalStatus = Some("Alive"), ageAtEventDays = Some(100))
    val outcome12 = EOutcome(kfId = Some("outcome_12"), participantId = Some("participant_id_1"), vitalStatus = Some("Deceased"), ageAtEventDays = Some(200))
    val outcome13 = EOutcome(kfId = Some("outcome_13"), participantId = Some("participant_id_1"), vitalStatus = Some("Alive"), ageAtEventDays = Some(200))


    val p2 = Participant_ES(kf_id = Some("participant_id_2"))
    val outcome21 = EOutcome(kfId = Some("outcome_21"), participantId = Some("participant_id_2"), vitalStatus = Some("Alive"), diseaseRelated = Some("NA"), ageAtEventDays = Some(50))
    //    val outcome22 = EOutcome(kfId = Some("outcome_22"), participantId = Some("participant_id_2"), vitalStatus = Some("Deceased"), diseaseRelated = Some("NA"), ageAtEventDays = None)
    val p3 = Participant_ES(kf_id = Some("participant_id_3"))

    val entityDataset = buildEntityDataSet(
        outcomes = Seq(outcome11, outcome12, outcome13, outcome21)
    )

    val result = MergeOutcome(entityDataset, Seq(p1, p2, p3).toDS()).collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("participant_id_1"),
        outcome = Some(Outcome_ES(age_at_event_days = Some(200), kf_id = Some("outcome_12"), vital_status = Some("Deceased")))
      ),
      Participant_ES(kf_id = Some("participant_id_2"),
        outcome = Some(Outcome_ES(age_at_event_days = Some(50), kf_id = Some("outcome_21"), vital_status = Some("Alive"), disease_related = Some("NA")))
      ),

      Participant_ES(kf_id = Some("participant_id_3"), outcome = Some(Outcome_ES()))
    )
  }


}
