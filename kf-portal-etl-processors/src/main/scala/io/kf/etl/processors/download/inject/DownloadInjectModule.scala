package io.kf.etl.processors.download.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.Constants._
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.context.Context
import io.kf.etl.processors.common.inject.ProcessorInjectModule
import io.kf.etl.processors.download.DownloadProcessor
import io.kf.etl.processors.download.context.{DownloadConfig, DownloadContext}
import io.kf.etl.processors.download.output.DownloadOutput
import io.kf.etl.processors.download.sink.DownloadSink
import io.kf.etl.processors.download.source.DownloadSource
import io.kf.etl.processors.download.transform.DownloadTransformer
import scala.util.{Failure, Success, Try}

@GuiceModule(name = "download")
class DownloadInjectModule(config: Option[Config]) extends ProcessorInjectModule(config) {
  type CONTEXT = DownloadContext
  type PROCESSOR = DownloadProcessor
  type SOURCE = DownloadSource
  type SINK = DownloadSink
  type TRANSFORMER = DownloadTransformer
  type OUTPUT = DownloadOutput

  override def getContext(): DownloadContext = {

    val cc = DownloadConfig(
      name = config.get.getString("name"),
      postgresql = Context.postgresql,
      dumpPath = getDumpPath(),
      dataPath = Try(config.get.getString(CONFIG_NAME_DATA_PATH)) match {
        case Success(path) => Some(path)
        case Failure(_) => None
      },
      mysql = Context.mysql


    )
    new DownloadContext(sparkSession, hdfs, appRootPath, cc)
  }

  private def getDumpPath():String = {
    // if dump_path is not available, get the local java temporary directory instead

    Try(config.get.getString(CONFIG_NAME_DUMP_PATH)) match {
      case Success(path) => path
      case Failure(_) => s"file://${System.getProperty("java.io.tmpdir")}/dump"
    }
  }

  @Provides
  override def getProcessor(): DownloadProcessor = {
    val context = getContext()
    val source = getSource(context)
    val sink = getSink(context)
    val transformer = getTransformer(context)
    val output = getOutput(context)

    new DownloadProcessor(
      context,
      source.getRepository,
      transformer.transform,
      sink.sink,
      output.output
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

  override def configure(): Unit = {}

  override def getOutput(context: DownloadContext): DownloadOutput = {
    new DownloadOutput(context)
  }
}
