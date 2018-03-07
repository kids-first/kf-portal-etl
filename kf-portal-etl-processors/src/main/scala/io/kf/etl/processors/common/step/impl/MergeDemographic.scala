package io.kf.etl.processors.common.step.impl

import io.kf.etl.model.Participant
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeDemographic(override val ctx: StepContext) extends StepExecutable[Dataset[Participant],Dataset[Participant]]{

  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
    import ctx.spark.implicits._
    val all = ctx.dbTables

    participants.joinWith(all.demographic, participants.col("kfId") === all.demographic.col("participantId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(dg) => {
          tuple._1.copy(
            race = tuple._2.race,
            ethnicity = tuple._2.ethnicity,
            gender = tuple._2.gender
          )
        }
        case None => tuple._1
      }
    })
  }
}

