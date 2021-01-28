package io.kf.etl.processors.tojson

import com.typesafe.config.Config
import io.kf.etl.common.Constants.JSON_OUTPUT_FILES
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.participantcommon.transform.step.WriteJsonSink
import org.apache.spark.sql.{Dataset, SparkSession}
import play.api.libs.ws.StandaloneWSClient


object JsonOutputProcessor {
  def apply[T](
                indexType: String,
                studyId: String,
                releaseId: String
              ) (dataset: Dataset[T])(implicit wsClient: StandaloneWSClient, config: Config, spark: SparkSession): Unit = {
    val partition = IndexProcessor.getIndexName(indexType, studyId, releaseId)
    WriteJsonSink.exportDataSetToJsonFile(config.getString(JSON_OUTPUT_FILES), partition)(dataset)
  }

}