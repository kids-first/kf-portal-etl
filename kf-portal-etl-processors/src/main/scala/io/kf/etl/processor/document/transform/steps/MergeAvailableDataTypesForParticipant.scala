package io.kf.etl.processor.document.transform.steps

import io.kf.etl.model.Participant
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

class MergeAvailableDataTypesForParticipant(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]]{
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
