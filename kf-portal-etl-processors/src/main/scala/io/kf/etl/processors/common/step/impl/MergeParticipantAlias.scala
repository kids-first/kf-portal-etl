package io.kf.etl.processors.common.step.impl

import io.kf.etl.model.Participant
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeParticipantAlias(override val ctx: StepContext) extends StepExecutable[Dataset[Participant],Dataset[Participant]] {
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
//    import ctx.parentContext.sparkSession.implicits._
//    val all = ctx.dbTables
//
//    participants.joinWith(all.participantAlis, participants.col("kfId") === all.participantAlis.col("kfId"), "left").groupByKey(tuple => {
//      tuple._1.kfId
//    }).mapGroups((id, iterator) => {
//      val list = iterator.toList
//      val filteredList = list.filter(_._2!= null)
//      if(filteredList.size == 0)
//        list(0)._1
//      else {
//        list(0)._1.copy(
//          ???
//        )
//      }
//    })
    ???
  }
}
