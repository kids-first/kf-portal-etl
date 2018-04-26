package io.kf.etl.processors.filecentric_new.inject

import com.typesafe.config.Config
import io.kf.etl.common.Constants.{CONFIG_NAME_DATA_PATH, CONFIG_NAME_WRITE_INTERMEDIATE_DATA}
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.processors.common.inject.ProcessorInjectModule
import io.kf.etl.processors.filecentric_new.context.FileCentricConfig
import io.kf.etl.processors.filecentric_new.FileCentricProcessor_New
import io.kf.etl.processors.filecentric_new.context.FileCentricContext
import io.kf.etl.processors.filecentric_new.output.FileCentricOutput
import io.kf.etl.processors.filecentric_new.sink.FileCentricSink
import io.kf.etl.processors.filecentric_new.source.FileCentricSource
import io.kf.etl.processors.filecentric_new.transform.FileCentricTransformer

import scala.util.{Failure, Success, Try}

@GuiceModule(name = "file_centric")
class FileCentricInjectModule(config: Option[Config]) extends ProcessorInjectModule(config) {
  override type CONTEXT = FileCentricContext
  override type PROCESSOR = FileCentricProcessor_New
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

    new FileCentricContext(sparkSession, hdfs, appRootPath, cc)
  }

  override def getProcessor(): FileCentricProcessor_New = {
    val context = getContext()
    new FileCentricProcessor_New(
      context,
      getSource(context).source,
      getTransformer(context).transform,
      getSink(context).sink,
      getOutput(context).output

    )
  }

  override def getSource(context: FileCentricContext): FileCentricSource = {
    new FileCentricSource(context)
  }

  override def getSink(context: FileCentricContext): FileCentricSink = {
    new FileCentricSink(context)
  }

  override def getTransformer(context: FileCentricContext): FileCentricTransformer = {
    new FileCentricTransformer(context)
  }

  override def getOutput(context: FileCentricContext): FileCentricOutput = {
    new FileCentricOutput(context)
  }

  override def configure(): Unit = {}
}
