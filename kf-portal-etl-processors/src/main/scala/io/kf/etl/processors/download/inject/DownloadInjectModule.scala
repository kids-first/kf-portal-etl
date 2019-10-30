package io.kf.etl.processors.download.inject

import com.google.inject.Provides
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
import play.api.libs.ws.StandaloneWSClient

import scala.util.{Failure, Success, Try}

@GuiceModule(name = "download")
class DownloadInjectModule(context: Context, moduleName: String) extends ProcessorInjectModule(context, moduleName) {
  type CONTEXT = DownloadContext
  type PROCESSOR = DownloadProcessor
  type SOURCE = DownloadSource
  type SINK = DownloadSink
  type TRANSFORMER = DownloadTransformer
  type OUTPUT = DownloadOutput

  override def getContext(): DownloadContext = {


    val cc = DownloadConfig(
      name = config.get.getString("name"),
      dataService = context.dataService,
      dumpPath = getDumpPath(),
      dataPath = Try(config.get.getString(CONFIG_NAME_DATA_PATH)) match {
        case Success(path) => Some(path)
        case Failure(_) => None
      },
      mondoPath = config.get.getString(CONFIG_NAME_MONDO_PATH),
      ncitPath = config.get.getString(CONFIG_NAME_NCIT_PATH),
      hpoPath = config.get.getString(CONFIG_NAME_HPO_PATH)
    )
    new DownloadContext(context, cc)
  }

  private def getDumpPath(): String = {
    // if dump_path is not available, get the local java temporary directory instead

    Try(config.get.getString(CONFIG_NAME_DUMP_PATH)) match {
      case Success(path) => path
      case Failure(_) => s"file://${System.getProperty("java.io.tmpdir")}/dump"
    }
  }

  @Provides
  override def getProcessor(): DownloadProcessor = {
    val dContext = getContext()
    val source = getSource(dContext)
    val sink = getSink(dContext)
    val transformer = getTransformer(dContext)
    val output = getOutput(dContext)

    new DownloadProcessor(
      dContext,
      source.getEntitySet,
      transformer.transform,
      sink.sink,
      output.output
    )
  }

  override def getSource(dContext: DownloadContext): DownloadSource = {
    new DownloadSource(dContext)
  }

  override def getSink(dContext: DownloadContext): DownloadSink = {
    new DownloadSink(dContext)
  }

  override def getTransformer(dContext: DownloadContext): DownloadTransformer = {
    //This should be instantiate in a more global place
    import scala.concurrent.ExecutionContext.Implicits._
    implicit val wsClient: StandaloneWSClient = context.getWsClient()
    new DownloadTransformer(dContext)
  }

  override def configure(): Unit = {}

  override def getOutput(dContext: DownloadContext): DownloadOutput = {
    new DownloadOutput(dContext)
  }
}
