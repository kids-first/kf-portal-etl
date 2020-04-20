package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.es.Participant_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object MergeStudy {
  def apply(entityDataset: EntityDataSet)(implicit spark: SparkSession): Dataset[Participant_ES] = {
    import spark.implicits._
    entityDataset.participants.joinWith(
      entityDataset.studies,
      entityDataset.participants.col("study_id") === entityDataset.studies.col("kf_id"),
      "left_outer"
    ).map(tuple => {
      val study = EntityConverter.EStudyToStudyES(tuple._2)
      EntityConverter.EParticipantToParticipantES(tuple._1).copy(study = Some(study))
    })
  }
}
