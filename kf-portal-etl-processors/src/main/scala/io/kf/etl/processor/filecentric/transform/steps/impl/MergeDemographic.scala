package io.kf.etl.processor.filecentric.transform.steps.impl

import io.kf.etl.model.filecentric.Participant
import io.kf.etl.processor.filecentric.transform.steps.StepExecutable
import io.kf.etl.processor.filecentric.transform.steps.context.FileCentricStepContext
import org.apache.spark.sql.Dataset

class MergeDemographic(override val ctx: FileCentricStepContext) extends StepExecutable[Dataset[Participant],Dataset[Participant]]{

  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
    import ctx.parentContext.sparkSession.implicits._
    val all = ctx.dbTables

    participants.joinWith(all.demographic, participants.col("kfId") === all.demographic.col("participantId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(dg) => {
          tuple._1.copy(
            isProband = tuple._1.isProband,
            consentType = tuple._1.consentType,
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

