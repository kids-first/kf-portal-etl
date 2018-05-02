package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeStudy(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]]{
  override def process(input: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.spark.implicits._
    ctx.entityDataset.participants.joinWith(
      ctx.entityDataset.studies,
      ctx.entityDataset.participants.col("studyId") === ctx.entityDataset.studies.col("kfId"),
      "left_outer"
    ).map(tuple => {
      val study = PBEntityConverter.EStudyToStudyES(tuple._2)
      PBEntityConverter.EParticipantToParticipantES(tuple._1).copy(study = Some(study))
    })
  }
}
