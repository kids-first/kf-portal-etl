package io.kf.etl.processor.document.transform.steps

import io.kf.etl.model.{Family, Participant}
import org.apache.spark.sql.Dataset

class MergeDemographicToParticipant(override val ctx: StepContext) extends StepExecutable[Dataset[Participant],Dataset[Participant]]{

  override def process(input: Dataset[Participant]): Dataset[Participant] = {
    import ctx.parentContext.sparkSession.implicits._
    val all = ctx.dbTables

    all.participant.joinWith(all.demographic, all.participant.col("kfId") === all.demographic.col("participantId"), "left").map(tuple => {
      val participant = Participant(
        kfId = tuple._1.kfId,
        uuid = tuple._1.uuid,
        createdAt = tuple._1.createdAt,
        modifiedAt = tuple._1.modifiedAt
      )
      Option(tuple._2) match {
        case Some(dg) => participant.copy(
          family = tuple._1.familyId match {
            case Some(id) => Some(Family(familyId = id))
            case None => None
          },
          isProband = tuple._1.isProband,
          consentType = tuple._1.consentType,
          race = tuple._2.race,
          ethnicity = tuple._2.ethnicity,
          gender = tuple._2.gender
        )
        case None => participant
      }
    })
  }
}

