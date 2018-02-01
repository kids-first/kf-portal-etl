package io.kf.etl.processor.document.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.processor.common.inject.ProcessorInjectModule
import io.kf.etl.processor.document.DocumentProcessor
import io.kf.etl.processor.document.context.DocumentContext
import io.kf.etl.processor.document.output.DocumentOutput
import io.kf.etl.processor.document.sink.DocumentSink
import io.kf.etl.processor.document.source.DocumentSource
import io.kf.etl.processor.document.transform.DocumentTransformer
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem => HDFS}


@GuiceModule(name = "document")
class DocumentInjectModule(sparkSession: SparkSession,
                           hdfs: HDFS,
                           appRootPath: String,
                           config: Option[Config]) extends ProcessorInjectModule(sparkSession, hdfs, appRootPath, config){
  type CONTEXT = DocumentContext
  type PROCESSOR = DocumentProcessor
  type SOURCE = DocumentSource
  type SINK = DocumentSink
  type TRANSFORMER = DocumentTransformer
  type OUTPUT = DocumentOutput

  override def configure(): Unit = ???

  override def getContext(): DocumentContext = {
    new DocumentContext(sparkSession, hdfs, appRootPath, config)
  }

  @Provides
  override def getProcessor(): DocumentProcessor = {
    val context = getContext()
    val source = getSource(context)
    val sink = getSink(context)
    val transformer = getTransformer(context)
    val output = getOutput(context)

    new DocumentProcessor(
      context,
      source.source,
      transformer.transform,
      sink.sink,
      output.output
    )

  }

  override def getSource(context: DocumentContext): DocumentSource = {
    new DocumentSource(context)
  }

  override def getSink(context: DocumentContext): DocumentSink = {
    new DocumentSink(context)
  }

  override def getTransformer(context: DocumentContext): DocumentTransformer = {
    new DocumentTransformer(sparkSession)
  }

  override def getOutput(context: DocumentContext): DocumentOutput = {
    new DocumentOutput(context)
  }
}