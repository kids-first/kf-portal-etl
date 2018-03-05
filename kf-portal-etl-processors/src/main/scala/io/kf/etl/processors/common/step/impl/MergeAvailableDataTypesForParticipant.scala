package io.kf.etl.processors.common.step.impl

import io.kf.etl.model.Participant
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeAvailableDataTypesForParticipant(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]]{
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
    import ctx.spark.implicits._

    participants.joinWith(ctx.dbTables.participantGenomicFile, participants.col("kfId") === ctx.dbTables.participantGenomicFile.col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(files) => tuple._1.copy(availableDataTypes = files.dataTypes)
        case None => tuple._1
      }
    })
  }

}
