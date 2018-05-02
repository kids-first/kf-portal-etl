package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeOutcome(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.spark.implicits._
    participants.joinWith(
      ctx.entityDataset.outcomes,
      participants.col("kfId") === ctx.entityDataset.outcomes.col("participantId"),
      "left_outer"
    ).map(tuple => {
      Option(tuple._2) match {
        case Some(outcome) => {
          tuple._1.copy(
            outcome = Some(PBEntityConverter.EOutcomeToOutcomeES(outcome))
          )
        }
        case None => tuple._1
      }

    })
  }
}
