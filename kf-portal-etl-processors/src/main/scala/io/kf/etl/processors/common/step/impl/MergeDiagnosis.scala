package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Diagnosis_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EDiagnosis
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.context.StepContext
import org.apache.spark.sql.Dataset

class MergeDiagnosis(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {

  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.entityDataset.diagnoses
    import ctx.spark.implicits._

    participants
      .joinWith(
        diagnoses,
        participants.col("kfId") === diagnoses.col("participantId"),
        "left_outer"
      )
      .as[(Participant_ES, Option[EDiagnosis])]
      .groupByKey { case (participant, _) => participant.kfId }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq: Seq[Diagnosis_ES] = groups.collect { case (_, Some(d)) => PBEntityConverter.EDiagnosisToDiagnosisES(d) }
        participant.copy(
          diagnoses = filteredSeq
        )
      })
  }


}
