package io.kf.etl.processors.participantcentric.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.Constants.{CONFIG_NAME_DATA_PATH, CONFIG_NAME_WRITE_INTERMEDIATE_DATA}
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.context.Context
import io.kf.etl.processors.common.inject.ProcessorInjectModule
import io.kf.etl.processors.participantcentric.ParticipantCentricProcessor
import io.kf.etl.processors.participantcentric.context.{ParticipantCentricConfig, ParticipantCentricContext}
import io.kf.etl.processors.participantcentric.output.ParticipantCentricOutput
import io.kf.etl.processors.participantcentric.sink.ParticipantCentricSink
import io.kf.etl.processors.participantcentric.source.ParticipantCentricSource
import io.kf.etl.processors.participantcentric.transform.ParticipantCentricTransformer

import scala.util.{Failure, Success, Try}

@GuiceModule(name = "participant_centric")
class ParticipantCentricInjectModule(context: Context, moduleName:String) extends ProcessorInjectModule(context, moduleName) {
  override type CONTEXT = ParticipantCentricContext
  override type PROCESSOR = ParticipantCentricProcessor
  override type SOURCE = ParticipantCentricSource
  override type SINK = ParticipantCentricSink
  override type TRANSFORMER = ParticipantCentricTransformer
  override type OUTPUT = ParticipantCentricOutput

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

    new ParticipantCentricContext(context, cc)
  }

  @Provides
  override def getProcessor(): ParticipantCentricProcessor = {
    val pContext = getContext()
    val source = getSource(pContext)
    val sink = getSink(pContext)
    val transformer = getTransformer(pContext)
    val output = getOutput(pContext)

    new ParticipantCentricProcessor(pContext, source.source, transformer.transform, sink.sink, output.output)

  }

  override def getSource(pContext: ParticipantCentricContext): ParticipantCentricSource = {
    new ParticipantCentricSource(pContext)
  }

  override def getSink(pContext: ParticipantCentricContext): ParticipantCentricSink = {
    new ParticipantCentricSink(pContext)
  }

  override def getTransformer(pContext: ParticipantCentricContext): ParticipantCentricTransformer = {
    new ParticipantCentricTransformer(pContext)
  }

  override def getOutput(pContext: ParticipantCentricContext): ParticipantCentricOutput = {
    new ParticipantCentricOutput(pContext)
  }

  override def configure(): Unit = {}
}
