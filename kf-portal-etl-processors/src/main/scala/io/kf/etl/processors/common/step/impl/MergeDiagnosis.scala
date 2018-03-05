package io.kf.etl.processors.common.step.impl

import io.kf.etl.model.{Diagnosis, Participant}
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeDiagnosis(override val ctx: StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]]{
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {

    import ctx.spark.implicits._

    participants.joinWith(ctx.dbTables.diagnosis, participants.col("kfId") === ctx.dbTables.diagnosis.col("participantId"), "left").groupByKey(_._1.kfId).mapGroups((parId, iterator) => {

      val list = iterator.toList
      list(0)._1.copy(
        diagnoses = {
          list.collect{
            case tuple if(tuple._2 != null) => {
              val tdia = tuple._2
              Diagnosis(
                kfId = tdia.kfId,
                uuid = tdia.uuid,
                createdAt = tdia.createdAt,
                modifiedAt = tdia.modifiedAt,
                diagnosis = tdia.diagnosis,
                ageAtEventDays = tdia.ageAtEventDays,
                tumorLocation = tdia.tumorLocation,
                diagnosisCategory = tdia.diagnosisCategory
              )
            }
          }
        }
      )
    })
  }
}
