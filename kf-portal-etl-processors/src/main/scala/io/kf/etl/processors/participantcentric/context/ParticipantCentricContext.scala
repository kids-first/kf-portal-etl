package io.kf.etl.processors.participantcentric.context

import io.kf.etl.common.Constants.PARTICIPANTCENTRIC_DEFAULT_DATA_PATH
import io.kf.etl.context.Context
import io.kf.etl.processors.common.processor.{ProcessorConfig, ProcessorContext}

class ParticipantCentricContext(override val appContext: Context,
                               override val config: ParticipantCentricConfig) extends ProcessorContext{
  def getProcessorDataPath():String = {
    config.dataPath match {
      case Some(cc) => cc
      case None => s"${appContext.rootPath}/${PARTICIPANTCENTRIC_DEFAULT_DATA_PATH}"
    }
  }

  def getProcessorSinkDataPath():String = {
    getProcessorDataPath() + "/sink"
  }

}

case class ParticipantCentricConfig(override val name:String, override val dataPath: Option[String], val write_intermediate_data: Boolean) extends ProcessorConfig