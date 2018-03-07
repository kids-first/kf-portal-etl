package io.kf.etl.processors.filecentric.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.processors.common.inject.ProcessorInjectModule
import io.kf.etl.processors.filecentric.FileCentricProcessor
import io.kf.etl.processors.filecentric.context.{FileCentricConfig, FileCentricContext}
import io.kf.etl.processors.filecentric.output.FileCentricOutput
import io.kf.etl.processors.filecentric.sink.FileCentricSink
import io.kf.etl.processors.filecentric.source.FileCentricSource
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem => HDFS}
import io.kf.etl.common.Constants._
import io.kf.etl.processors.filecentric.transform.FileCentricTransformer

import scala.util.{Failure, Success, Try}


@GuiceModule(name = "file_centric")
class FileCentricInjectModule(sparkSession: SparkSession,
                              hdfs: HDFS,
                              appRootPath: String,
                              config: Option[Config]) extends ProcessorInjectModule(sparkSession, hdfs, appRootPath, config){
  type CONTEXT = FileCentricContext
  type PROCESSOR = FileCentricProcessor
  type SOURCE = FileCentricSource
  type SINK = FileCentricSink
  type TRANSFORMER = FileCentricTransformer
  type OUTPUT = FileCentricOutput

  override def configure(): Unit = {}

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
}