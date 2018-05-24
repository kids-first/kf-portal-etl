package io.kf.etl.processors.participantcommon.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.Constants.{CONFIG_NAME_DATA_PATH, CONFIG_NAME_WRITE_INTERMEDIATE_DATA}
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.processors.common.inject.ProcessorInjectModule
import io.kf.etl.processors.participantcommon.ParticipantCommonProcessor
import io.kf.etl.processors.participantcommon.context.{ParticipantCommonConfig, ParticipantCommonContext}
import io.kf.etl.processors.participantcommon.output.ParticipantCommonOutput
import io.kf.etl.processors.participantcommon.sink.ParticipantCommonSink
import io.kf.etl.processors.participantcommon.source.ParticipantCommonSource
import io.kf.etl.processors.participantcommon.transform.ParticipantCommonTransformer

import scala.util.{Failure, Success, Try}

@GuiceModule(name = "participant_common")
class ParticipantCommonInject(config: Option[Config]) extends ProcessorInjectModule(config){
  override type CONTEXT = ParticipantCommonContext
  override type PROCESSOR = ParticipantCommonProcessor
  override type SOURCE = ParticipantCommonSource
  override type SINK = ParticipantCommonSink
  override type TRANSFORMER = ParticipantCommonTransformer
  override type OUTPUT = ParticipantCommonOutput

  override def getContext(): ParticipantCommonContext = {
    val cc = ParticipantCommonConfig(

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

    new ParticipantCommonContext(sparkSession, hdfs, appRootPath, cc, s3)
  }

  @Provides
  override def getProcessor(): ParticipantCommonProcessor = {
    val context = getContext()
    val source = getSource(context)
    val sink = getSink(context)
    val transformer = getTransformer(context)
    val output = getOutput(context)

    new ParticipantCommonProcessor(
      context,
      source.source,
      transformer.transform,
      sink.sink,
      output.output
    )
  }

  override def getSource(context: ParticipantCommonContext): ParticipantCommonSource = {
    new ParticipantCommonSource(context)
  }

  override def getSink(context: ParticipantCommonContext): ParticipantCommonSink = {
    new ParticipantCommonSink(context)
  }

  override def getTransformer(context: ParticipantCommonContext): ParticipantCommonTransformer = {
    new ParticipantCommonTransformer(context)
  }

  override def getOutput(context: ParticipantCommonContext): ParticipantCommonOutput = {
    new ParticipantCommonOutput(context)
  }

  override def configure(): Unit = {}
}
