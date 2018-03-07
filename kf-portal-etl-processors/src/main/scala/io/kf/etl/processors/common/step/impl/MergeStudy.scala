package io.kf.etl.processors.common.step.impl

import io.kf.etl.model.{Family, Participant, Study}
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeStudy(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]] {
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
    import ctx.spark.implicits._
    val all = ctx.dbTables
    all.participant.joinWith(all.study, all.participant.col("studyId") === all.study.col("kfId"), "left").map(tuple => {
      Participant(
        kfId = tuple._1.kfId,
        uuid = tuple._1.uuid,
        createdAt = tuple._1.createdAt,
        modifiedAt = tuple._1.modifiedAt,
        isProband = tuple._1.isProband,
        consentType = tuple._1.consentType,
        family = tuple._1.familyId match {
          case Some(id) => Some(Family(familyId = id))
          case None => None
        },
        study = Some(
          Study(
            kfId = tuple._2.kfId,
            uuid = tuple._2.uuid,
            createdAt = tuple._2.createdAt,
            modifiedAt = tuple._2.modifiedAt,
            dataAccessAuthority = tuple._2.dataAccessAuthority,
            externalId = tuple._2.externalId,
            version = tuple._2.version,
            name = tuple._2.name,
            attribution = tuple._2.attribution
          )
        )
      )
    })
  }
}
