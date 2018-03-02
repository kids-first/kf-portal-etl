package io.kf.etl.processor.filecentric.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.processor.common.inject.ProcessorInjectModule
import io.kf.etl.processor.filecentric.FileCentricProcessor
import io.kf.etl.processor.filecentric.context.{FileCentricConfig, DocumentContext}
import io.kf.etl.processor.filecentric.output.FileCentricOutput
import io.kf.etl.processor.filecentric.sink.FileCentricSink
import io.kf.etl.processor.filecentric.source.FileCentricSource
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem => HDFS}
import io.kf.etl.common.Constants._
import io.kf.etl.processor.filecentric.transform.FileCentricTransformer

import scala.util.{Failure, Success, Try}


@GuiceModule(name = "filecentric")
class FileCentricInjectModule(sparkSession: SparkSession,
                              hdfs: HDFS,
                              appRootPath: String,
                              config: Option[Config]) extends ProcessorInjectModule(sparkSession, hdfs, appRootPath, config){
  type CONTEXT = DocumentContext
  type PROCESSOR = FileCentricProcessor
  type SOURCE = FileCentricSource
  type SINK = FileCentricSink
  type TRANSFORMER = FileCentricTransformer
  type OUTPUT = FileCentricOutput

  override def configure(): Unit = {}

  override def getContext(): DocumentContext = {

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

    new DocumentContext(sparkSession, hdfs, appRootPath, cc)
  }

  @Provides
  override def getProcessor(): FileCentricProcessor = {
    val context = getContext()
    val source = getSource(context)
    val sink = getSink(context)
    val transformer = getTransformer(context)
    val output = getOutput(context)

    new FileCentricProcessor(
      context,
      source.source,
      transformer.transform,
      sink.sink,
      output.output
    )

  }

  override def getSource(context: DocumentContext): FileCentricSource = {
    new FileCentricSource(context)
  }

  override def getSink(context: DocumentContext): FileCentricSink = {
    new FileCentricSink(context)
  }

  override def getTransformer(context: DocumentContext): FileCentricTransformer = {
    new FileCentricTransformer(context)
  }

  override def getOutput(context: DocumentContext): FileCentricOutput = {
    new FileCentricOutput(context)
  }
}