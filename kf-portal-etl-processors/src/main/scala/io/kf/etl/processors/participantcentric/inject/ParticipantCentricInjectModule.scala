package io.kf.etl.processors.participantcentric.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.Constants._
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.processors.common.inject.ProcessorInjectModule
import io.kf.etl.processors.participantcentric.ParticipantCentricProcessor
import io.kf.etl.processors.participantcentric.context.{ParticipantCentricConfig, ParticipantCentricContext}
import io.kf.etl.processors.participantcentric.output.ParticipantCentricOutput
import io.kf.etl.processors.participantcentric.sink.ParticipantCentricSink
import io.kf.etl.processors.participantcentric.source.ParticipantCentricSource
import io.kf.etl.processors.participantcentric.transform.ParticipantCentricTransformer
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem => HDFS}

import scala.util.{Failure, Success, Try}

@GuiceModule(name = "participant_centric")
class ParticipantCentricInjectModule(sparkSession: SparkSession,
                                     hdfs: HDFS,
                                     appRootPath: String,
                                     config: Option[Config]) extends ProcessorInjectModule(sparkSession, hdfs, appRootPath, config) {
  type CONTEXT = ParticipantCentricContext
  type PROCESSOR = ParticipantCentricProcessor
  type OUTPUT = ParticipantCentricOutput
  type SINK = ParticipantCentricSink
  type SOURCE = ParticipantCentricSource
  type TRANSFORMER = ParticipantCentricTransformer

  override def configure(): Unit = {}

  override def getContext(): ParticipantCentricContext = {
    val cc = ParticipantCentricConfig(
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

    new ParticipantCentricContext(sparkSession, hdfs, appRootPath, cc)
  }

  @Provides
  override def getProcessor(): ParticipantCentricProcessor = {
    val context = getContext()
    val source = getSource(context)
    val sink = getSink(context)
    val transformer = getTransformer(context)
    val output = getOutput(context)

    new ParticipantCentricProcessor(context, source.source, transformer.transform, sink.sink, output.output )
  }

  override def getSource(context: ParticipantCentricContext): ParticipantCentricSource = {
    new ParticipantCentricSource(context)
  }

  override def getSink(context: ParticipantCentricContext): ParticipantCentricSink = {
    new ParticipantCentricSink(context)
  }

  override def getTransformer(context: ParticipantCentricContext): ParticipantCentricTransformer = {
    new ParticipantCentricTransformer(context)
  }

  override def getOutput(context: ParticipantCentricContext): ParticipantCentricOutput = {
    new ParticipantCentricOutput(context)
  }
}
