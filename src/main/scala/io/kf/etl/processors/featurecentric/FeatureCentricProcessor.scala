package io.kf.etl.processors.featurecentric

import com.typesafe.config.Config
import io.kf.etl.models.es.{FileCentric_ES, ParticipantCentric_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.featurecentric.transform.FeatureCentricTransformer
import org.apache.spark.sql.{Dataset, SparkSession}

object FeatureCentricProcessor {
  def participantCentric(entityDataSet: EntityDataSet, participants: Dataset[Participant_ES])(implicit config: Config, spark: SparkSession): Dataset[ParticipantCentric_ES] = {
    val transformed = FeatureCentricTransformer.participantCentric(entityDataSet, participants)//.cache()
//    WriteParquetSink("participant_centric", transformed)
    transformed
  }

  def fileCentric(entityDataSet: EntityDataSet, participants: Dataset[Participant_ES])(implicit config: Config, spark: SparkSession): Dataset[FileCentric_ES] = {
    val transformed = FeatureCentricTransformer.fileCentric(entityDataSet, participants)//.cache()
//    WriteParquetSink("file_centric", transformed)
    transformed
  }
}
