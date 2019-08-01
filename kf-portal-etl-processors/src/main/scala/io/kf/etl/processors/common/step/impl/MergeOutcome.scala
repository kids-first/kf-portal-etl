package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.external.dataservice.entity.EOutcome
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.context.StepContext
import org.apache.spark.sql.Dataset

class MergeOutcome(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.spark.implicits._
    participants.joinWith(
      ctx.entityDataset.outcomes,
      participants.col("kfId") === ctx.entityDataset.outcomes.col("participantId"),
      "left_outer"
    )
      .as[(Participant_ES, Option[EOutcome])]
      .groupByKey { case (participant, _) => participant.kfId }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1


        val outcomes = groups.flatMap(_._2)
        if (outcomes.nonEmpty) {
          val deceasedOutcome: Option[EOutcome] = outcomes.find(o => o.vitalStatus.contains("Deceased"))
          deceasedOutcome match {
            case None =>
              val lastOutcome = outcomes.maxBy(_.ageAtEventDays)
              participant.copy(
                outcome = Some(PBEntityConverter.EOutcomeToOutcomeES(lastOutcome))
              )
            case Some(o) => participant.copy(outcome = Some(PBEntityConverter.EOutcomeToOutcomeES(o)))
          }

        } else
          participant
      })

  }
}
