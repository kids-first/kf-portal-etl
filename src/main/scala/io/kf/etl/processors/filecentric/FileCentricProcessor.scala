package io.kf.etl.processors.filecentric

import com.typesafe.config.Config
import io.kf.etl.models.es.{FileCentric_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.sink.WriteParquetSink
import io.kf.etl.processors.filecentric.transform.FileCentricTransformer
import org.apache.spark.sql.{Dataset, SparkSession}


object FileCentricProcessor {
  def apply(entityDataSet: EntityDataSet, participants: Dataset[Participant_ES])(implicit config: Config, spark: SparkSession): Dataset[FileCentric_ES] = {
    val transformed = FileCentricTransformer(entityDataSet, participants).cache()
    WriteParquetSink("file_centric", transformed)
    transformed
  }
}
