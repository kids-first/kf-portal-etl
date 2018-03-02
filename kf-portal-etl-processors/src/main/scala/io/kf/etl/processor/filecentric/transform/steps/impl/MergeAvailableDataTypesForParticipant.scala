package io.kf.etl.processor.filecentric.transform.steps.impl

import io.kf.etl.model.filecentric.Participant
import io.kf.etl.processor.filecentric.transform.steps.StepExecutable
import io.kf.etl.processor.filecentric.transform.steps.context.FileCentricStepContext
import org.apache.spark.sql.Dataset

class MergeAvailableDataTypesForParticipant(override val ctx:FileCentricStepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]]{
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
    import ctx.parentContext.sparkSession.implicits._

    participants.joinWith(ctx.participant2GenomicFiles, participants.col("kfId") === ctx.participant2GenomicFiles.col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(files) => tuple._1.copy(availableDataTypes = files.dataTypes)
        case None => tuple._1
      }
    })
  }

}
