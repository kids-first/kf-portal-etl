package io.kf.etl.processors.participantcentric.context

import io.kf.etl.common.Constants.PARTICIPANTCENTRIC_DEFAULT_DATA_PATH
import io.kf.etl.processors.common.processor.{ProcessorConfig, ProcessorContext}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem => HDFS}

class ParticipantCentricContext(override val sparkSession: SparkSession,
                                override val hdfs: HDFS,
                                override val appRootPath: String,
                                override val config: ParticipantCentricConfig) extends ProcessorContext{
  def getProcessorDataPath():String = {
    config.dataPath match {
      case Some(cc) => cc
      case None => s"${appRootPath}/${PARTICIPANTCENTRIC_DEFAULT_DATA_PATH}"
    }
  }

  def getProcessorSinkDataPath():String = {
    getProcessorDataPath() + "/sink"
  }
}

case class ParticipantCentricConfig(override val name:String, override val dataPath: Option[String], val write_intermediate_data: Boolean) extends ProcessorConfig