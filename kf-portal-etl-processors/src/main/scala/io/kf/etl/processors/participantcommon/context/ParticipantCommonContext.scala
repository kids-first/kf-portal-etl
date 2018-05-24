package io.kf.etl.processors.participantcommon.context

import com.amazonaws.services.s3.AmazonS3
import io.kf.etl.common.Constants.PARTICIPANT_COMMON_DEFAULT_DATA_PATH
import io.kf.etl.processors.common.processor.{ProcessorConfig, ProcessorContext}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem => HDFS}


class ParticipantCommonContext(override val sparkSession: SparkSession,
                               override val hdfs: HDFS,
                               override val appRootPath: String,
                               override val config: ParticipantCommonConfig,
                               override val s3: AmazonS3) extends ProcessorContext{
  def getProcessorDataPath():String = {
    config.dataPath match {
      case Some(cc) => cc
      case None => s"${appRootPath}/${PARTICIPANT_COMMON_DEFAULT_DATA_PATH}"
    }
  }

  def getProcessorSinkDataPath():String = {
    getProcessorDataPath() + "/sink"
  }
}

case class ParticipantCommonConfig(override val name:String, override val dataPath: Option[String], val write_intermediate_data: Boolean) extends ProcessorConfig