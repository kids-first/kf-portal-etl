package io.kf.etl.processor.download.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.processor.common.inject.ProcessorInjectModule
import io.kf.etl.processor.download.DownloadProcessor
import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.download.sink.DownloadSink
import io.kf.etl.processor.download.source.DownloadSource
import io.kf.etl.processor.download.transform.DownloadTransformer
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem => HDFS}

@GuiceModule(name = "download")
class DownloadInjectModule(sparkSession: SparkSession,
                           hdfs: HDFS,
                           appRootPath: String,
                           config: Option[Config]) extends ProcessorInjectModule(sparkSession, hdfs, appRootPath, config) {
  type CONTEXT = DownloadContext
  type PROCESSOR = DownloadProcessor
  type SOURCE = DownloadSource
  type SINK = DownloadSink
  type TRANSFORMER = DownloadTransformer

  override def getContext(): DownloadContext = {
    new DownloadContext(sparkSession, hdfs, appRootPath, config)
  }

  @Provides
  override def getProcessor(): DownloadProcessor = {
    val context = getContext()
    val source = getSource(context)
    val sink = getSink(context)
    val transformer = getTransformer(context)

    new DownloadProcessor(
      context,
      source.getRepository,
      transformer.transform,
      sink.sink
    )
  }

  override def getSource(context: DownloadContext): DownloadSource = {
    new DownloadSource(context)
  }

  override def getSink(context: DownloadContext): DownloadSink = {
    new DownloadSink(context)
  }

  override def getTransformer(context: DownloadContext): DownloadTransformer = {
    new DownloadTransformer(context)
  }

  override def configure(): Unit = ???
}
