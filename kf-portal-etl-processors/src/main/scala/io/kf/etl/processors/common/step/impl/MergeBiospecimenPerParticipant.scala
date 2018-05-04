package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

/**
  * merge all of the biospecimens per participant
  * @param ctx
  */
class MergeBiospecimenPerParticipant(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.spark.implicits._
    participants.joinWith(
      ctx.entityDataset.biospecimens,
      participants.col("kfId") === ctx.entityDataset.biospecimens.col("participantId"),
      "left_outer"
    ).groupByKey(tuple => {
      tuple._1.kfId.get
    }).mapGroups((_, iterator ) => {
      val seq = iterator.toSeq
      val participant = seq(0)._1
      val filteredSeq = seq.filter(_._2 != null)
      participant.copy(
        biospecimens = filteredSeq.map(tuple => PBEntityConverter.EBiospecimenToBiospecimenES(tuple._2))
      )
    })
  }
}
