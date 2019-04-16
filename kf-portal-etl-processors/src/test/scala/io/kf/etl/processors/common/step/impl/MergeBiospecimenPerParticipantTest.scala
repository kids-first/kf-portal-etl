package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Biospecimen_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EBiospecimen
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.{StepContextUtil, WithSparkSession}
import org.scalatest.{FlatSpec, Matchers}

class MergeBiospecimenPerParticipantTest extends FlatSpec with Matchers with WithSparkSession {
  import spark.implicits._
  "process" should "join biospecimen and participant and enrich the ncid tissue tupe and anatomical site" in {

    val p1 = Participant_ES(kfId = Some("participant_id_1"))
    val bioSpecimen1 = EBiospecimen(kfId = Some("biospecimen_id_1"), participantId = Some("participant_id_1"))

    val p2 = Participant_ES(kfId = Some("participant_id_2"))
    val bioSpecimen21 = EBiospecimen(kfId = Some("biospecimen_id_21"), participantId = Some("participant_id_2"))
    val bioSpecimen22 = EBiospecimen(kfId = Some("biospecimen_id_22"), participantId = Some("participant_id_2"))

    val bioSpecimen3 = EBiospecimen(kfId = Some("biospecimen_id_3"), participantId = None) //should be ignore, no participants

    val p3 = Participant_ES(kfId = Some("participant_id_3"))

    val participantsDataset = Seq(p1, p2, p3).toDS()
    val entityDataset = buildEntityDataSet(
      biospecimens = Seq(bioSpecimen1, bioSpecimen21, bioSpecimen22, bioSpecimen3)
    )

    val mergeBiospecimen = new MergeBiospecimenPerParticipant(ctx = StepContextUtil.buildContext(entityDataset))

    val result = mergeBiospecimen.process(participantsDataset).collect()
    result should contain theSameElementsAs Seq(
      Participant_ES(kfId = Some("participant_id_1"),
        biospecimens = Seq(Biospecimen_ES(kfId = Some("biospecimen_id_1")))
      ),
      Participant_ES(kfId = Some("participant_id_2"),
        biospecimens = Seq(
          Biospecimen_ES(kfId = Some("biospecimen_id_22")),
          Biospecimen_ES(kfId = Some("biospecimen_id_21")))),
      Participant_ES(kfId = Some("participant_id_3"))
    )


  }
}
