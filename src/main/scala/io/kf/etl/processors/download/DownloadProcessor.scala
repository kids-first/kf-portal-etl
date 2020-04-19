package io.kf.etl.processors.download

import akka.actor.ActorSystem
import com.typesafe.config.Config
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.download.sink.DownloadSink
import io.kf.etl.processors.download.source.DownloadSource
import io.kf.etl.processors.download.transform.DownloadTransformer
import org.apache.spark.sql.SparkSession
import play.api.libs.ws.StandaloneWSClient

import scala.concurrent.ExecutionContext

object DownloadProcessor {
  def apply(studyId: String)(implicit WSClient: StandaloneWSClient, ec: ExecutionContext, spark: SparkSession, config: Config, system: ActorSystem): EntityDataSet = {
    val source = DownloadSource.getEntitySet(studyId)
    val transformer = new DownloadTransformer()
    val transformed = transformer.transform(source)
    DownloadSink(transformed, studyId)
    transformed

  }
}