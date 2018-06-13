package io.kf.etl.processors.filecentric.inject

import com.google.inject.Provides
import io.kf.etl.common.Constants.{CONFIG_NAME_DATA_PATH, CONFIG_NAME_WRITE_INTERMEDIATE_DATA}
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.context.Context
import io.kf.etl.processors.common.inject.ProcessorInjectModule
import io.kf.etl.processors.filecentric.context.FileCentricConfig
import io.kf.etl.processors.filecentric.FileCentricProcessor
import io.kf.etl.processors.filecentric.context.FileCentricContext
import io.kf.etl.processors.filecentric.output.FileCentricOutput
import io.kf.etl.processors.filecentric.sink.FileCentricSink
import io.kf.etl.processors.filecentric.source.FileCentricSource
import io.kf.etl.processors.filecentric.transform.FileCentricTransformer

import scala.util.{Failure, Success, Try}

@GuiceModule(name = "file_centric")
class FileCentricInjectModule(context: Context, moduleName:String) extends ProcessorInjectModule(context, moduleName) {
  override type CONTEXT = FileCentricContext
  override type PROCESSOR = FileCentricProcessor
  override type SOURCE = FileCentricSource
  override type SINK = FileCentricSink
  override type TRANSFORMER = FileCentricTransformer
  override type OUTPUT = FileCentricOutput

  override def getContext(): FileCentricContext = {
    val cc = FileCentricConfig(
      config.get.getString("name"),
      Try(config.get.getString(CONFIG_NAME_DATA_PATH)) match {
        case Success(path) => Some(path)
        case Failure(_) => None
      },
      Try(config.get.getBoolean(CONFIG_NAME_WRITE_INTERMEDIATE_DATA)) match {
        case Success(bWrite) => bWrite
        case Failure(_) => false
      }
    )

    new FileCentricContext(context, cc)
  }

  @Provides
  override def getProcessor(): FileCentricProcessor = {
    val fContext = getContext()
    new FileCentricProcessor(
      fContext,
      getSource(fContext).source,
      getTransformer(fContext).transform,
      getSink(fContext).sink,
      getOutput(fContext).output

    )
  }

  override def getSource(fContext: FileCentricContext): FileCentricSource = {
    new FileCentricSource(fContext)
  }

  override def getSink(fContext: FileCentricContext): FileCentricSink = {
    new FileCentricSink(fContext)
  }

  override def getTransformer(fContext: FileCentricContext): FileCentricTransformer = {
    new FileCentricTransformer(fContext)
  }

  override def getOutput(fContext: FileCentricContext): FileCentricOutput = {
    new FileCentricOutput(fContext)
  }

  override def configure(): Unit = {}
}
