package io.kf.etl.processors.participantcommon.inject

import com.google.inject.Provides
import io.kf.etl.common.Constants.{CONFIG_NAME_DATA_PATH, CONFIG_NAME_WRITE_INTERMEDIATE_DATA}
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.context.Context
import io.kf.etl.processors.common.inject.ProcessorInjectModule
import io.kf.etl.processors.participantcommon.ParticipantCommonProcessor
import io.kf.etl.processors.participantcommon.context.{ParticipantCommonConfig, ParticipantCommonContext}
import io.kf.etl.processors.participantcommon.output.ParticipantCommonOutput
import io.kf.etl.processors.participantcommon.sink.ParticipantCommonSink
import io.kf.etl.processors.participantcommon.source.ParticipantCommonSource
import io.kf.etl.processors.participantcommon.transform.ParticipantCommonTransformer

import scala.util.{Failure, Success, Try}

@GuiceModule(name = "participant_common")
class ParticipantCommonInject(context: Context, moduleName:String) extends ProcessorInjectModule(context, moduleName){
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

    new ParticipantCommonContext(context, cc)
  }

  @Provides
  override def getProcessor(): ParticipantCommonProcessor = {
    val pcContext = getContext()
    val source = getSource(pcContext)
    val sink = getSink(pcContext)
    val transformer = getTransformer(pcContext)
    val output = getOutput(pcContext)

    new ParticipantCommonProcessor(
      pcContext,
      source.source,
      transformer.transform,
      sink.sink,
      output.output
    )
  }

  override def getSource(pcContext: ParticipantCommonContext): ParticipantCommonSource = {
    new ParticipantCommonSource(pcContext)
  }

  override def getSink(pcContext: ParticipantCommonContext): ParticipantCommonSink = {
    new ParticipantCommonSink(pcContext)
  }

  override def getTransformer(pcContext: ParticipantCommonContext): ParticipantCommonTransformer = {
    new ParticipantCommonTransformer(pcContext)
  }

  override def getOutput(pcContext: ParticipantCommonContext): ParticipantCommonOutput = {
    new ParticipantCommonOutput(pcContext)
  }

  override def configure(): Unit = {}
}
