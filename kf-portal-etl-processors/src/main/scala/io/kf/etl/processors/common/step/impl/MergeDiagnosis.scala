package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Diagnosis_ES, Participant_ES}
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeDiagnosis(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.spark.implicits._
    participants.joinWith(
      ctx.entityDataset.diagnoses,
      participants.col("kfId") === ctx.entityDataset.diagnoses.col("participantId"),
      "left_outer"
    ).groupByKey(tuple => {
      tuple._1.kfId.get
    }).mapGroups((_, iterator) => {
      val seq = iterator.toSeq

      val participant = seq(0)._1

      val filteredSeq = seq.filter(_._2 != null)

      val diagnosesList:Seq[Diagnosis_ES] = filteredSeq.size match {
        case 0 => Seq(Diagnosis_ES())
        case _ => seq.map(tuple => PBEntityConverter.EDiagnosisToDiagnosisES(tuple._2))
      }

      participant.copy(
        diagnoses = diagnosesList
      )

    })
  }

}
