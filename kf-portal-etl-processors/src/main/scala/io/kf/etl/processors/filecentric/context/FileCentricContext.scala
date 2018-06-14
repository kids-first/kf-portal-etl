package io.kf.etl.processors.filecentric.context

import io.kf.etl.common.Constants.FILECENTRIC_DEFAULT_DATA_PATH
import io.kf.etl.context.Context
import io.kf.etl.processors.common.processor.{ProcessorConfig, ProcessorContext}

case class FileCentricContext(override val appContext: Context,
                              override val config: FileCentricConfig) extends ProcessorContext{
  def getProcessorDataPath():String = {
    config.dataPath match {
      case Some(cc) => cc
      case None => s"${appContext.rootPath}/${FILECENTRIC_DEFAULT_DATA_PATH}"
    }
  }

  def getProcessorSinkDataPath():String = {
    getProcessorDataPath() + "/sink"
  }

}


case class FileCentricConfig(override val name:String, override val dataPath: Option[String], val write_intermediate_data: Boolean) extends ProcessorConfig