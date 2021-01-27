package io.kf.etl.processors.tojson

import com.typesafe.config.Config
import io.kf.etl.common.Constants.JSON_OUTPUT_FILES
import io.kf.etl.processors.participantcommon.transform.step.WriteJsonSink
import org.apache.spark.sql.{Dataset, SparkSession}
import play.api.libs.ws.StandaloneWSClient


object JsonOutputProcessor {
  def apply[T](datasets: Map[String, Dataset[T]])(implicit wsClient: StandaloneWSClient, config: Config, spark: SparkSession): Unit = {

    datasets.foreach(d =>
      WriteJsonSink[T](config.getString(JSON_OUTPUT_FILES))(d._2)
    )
  }

}