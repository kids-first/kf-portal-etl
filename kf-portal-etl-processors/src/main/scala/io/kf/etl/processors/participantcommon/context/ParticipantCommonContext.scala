package io.kf.etl.processors.participantcommon.context

import io.kf.etl.common.Constants.PARTICIPANT_COMMON_DEFAULT_DATA_PATH
import io.kf.etl.context.Context
import io.kf.etl.processors.common.processor.{ProcessorConfig, ProcessorContext}

class ParticipantCommonContext(override val appContext: Context,
                               override val config: ParticipantCommonConfig) extends ProcessorContext{
  def getProcessorDataPath():String = {
    config.dataPath match {
      case Some(cc) => cc
      case None => s"${appContext.rootPath}/${PARTICIPANT_COMMON_DEFAULT_DATA_PATH}"
    }
  }

  def getProcessorSinkDataPath():String = {
    getProcessorDataPath() + "/sink"
  }

}

case class ParticipantCommonConfig(override val name:String, override val dataPath: Option[String], val write_intermediate_data: Boolean) extends ProcessorConfig