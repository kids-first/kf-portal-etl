package io.kf.etl.processor.document.transform.steps

import io.kf.etl.model.{Participant, Study}
import org.apache.spark.sql.Dataset

class MergeStudy(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]] {
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
    import ctx.parentContext.sparkSession.implicits._
    participants.joinWith(ctx.dbTables.study, participants.col("kfId") === ctx.dbTables.study.col("participantId"), "left").groupByKey(_._1.kfId).mapGroups((parId, iterator) => {

      val list = iterator.toList

      list(0)._1.copy(
        studies = {
          list.collect{
            case tuple if(tuple._2 != null) => {
              val ts = tuple._2
              Study(
                kfId = ts.kfId,
                uuid = ts.uuid,
                createdAt = ts.createdAt,
                modifiedAt = ts.modifiedAt,
                dataAccessAuthority = ts.dataAccessAuthority,
                externalId = ts.externalId,
                version = ts.version,
                name = ts.name,
                attribution = ts.attribution
              )
            }
          }
        }
      )
    })
  }
}
