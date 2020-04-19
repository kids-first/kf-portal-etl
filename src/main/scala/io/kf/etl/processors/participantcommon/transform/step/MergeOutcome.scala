package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.EOutcome
import io.kf.etl.models.es.{Outcome_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object MergeOutcome {
  def apply(entityDataset: EntityDataSet, participants: Dataset[Participant_ES])(implicit spark: SparkSession): Dataset[Participant_ES] = {
    import spark.implicits._

    participants.joinWith(
      entityDataset.outcomes,
      participants.col("kf_id") === entityDataset.outcomes.col("participant_id"),
      "left_outer"
    )
      .as[(Participant_ES, Option[EOutcome])]
      .groupByKey { case (participant, _) => participant.kf_id }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1


        val outcomes = groups.flatMap(_._2)
        if (outcomes.nonEmpty) {
          val deceasedOutcome: Option[EOutcome] = outcomes.find(o => o.vital_status.contains("Deceased"))
          deceasedOutcome match {
            case None =>
              val lastOutcome = outcomes.maxBy(_.age_at_event_days)
              participant.copy(
                outcome = Some(EntityConverter.EOutcomeToOutcomeES(lastOutcome))
              )
            case Some(o) => participant.copy(outcome = Some(EntityConverter.EOutcomeToOutcomeES(o)))
          }

        } else {
          //The survival graph need an empty outcome to work properly
          participant.copy(outcome = Some(Outcome_ES()))
        }

      })

  }
}
