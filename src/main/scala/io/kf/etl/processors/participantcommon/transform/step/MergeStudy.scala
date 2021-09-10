package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.{EParticipant, EStudy}
import io.kf.etl.models.es.{Participant_ES, Study_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object MergeStudy {
  def apply(entityDataset: EntityDataSet)(implicit spark: SparkSession): Dataset[Participant_ES] = {
    import spark.implicits._
    // Visible studies need to be filtered here because dataservice endpoint /studies/STUDY_ID?visible=true
    // does not filter only studies are visible
    val visibleStudies = entityDataset.studies.filter(s => s.visible.getOrElse(false))
    val joined: Dataset[(EParticipant, EStudy)] = entityDataset.participants.joinWith(
      visibleStudies,
      entityDataset.participants.col("study_id") === entityDataset.studies.col("kf_id"),
      "left_outer"
    )
    joined.map { case (participant, study) =>
      val studyES: Study_ES = EntityConverter.EStudyToStudyES(study)
      EntityConverter.EParticipantToParticipantES(participant).copy(study = Some(studyES))
    }
  }
}
