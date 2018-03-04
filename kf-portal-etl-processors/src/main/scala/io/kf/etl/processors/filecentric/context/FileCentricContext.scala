package io.kf.etl.processors.filecentric.context

import io.kf.etl.common.Constants._
import io.kf.etl.processors.common.processor.{ProcessorConfig, ProcessorContext}
import org.apache.hadoop.fs.{FileSystem => HDFS}
import org.apache.spark.sql.SparkSession


case class FileCentricContext(override val sparkSession: SparkSession,
                              override val hdfs: HDFS,
                              override val appRootPath: String,
                              override val config: FileCentricConfig) extends ProcessorContext{
  def getProcessorDataPath():String = {
    config.dataPath match {
      case Some(cc) => cc
      case None => s"${appRootPath}/${FILECENTRIC_DEFAULT_DATA_PATH}"
    }
  }

  def getProcessorSinkDataPath():String = {
    getProcessorDataPath() + "/sink"
  }
}


case class FileCentricConfig(override val name:String, override val dataPath: Option[String], val write_intermediate_data: Boolean) extends ProcessorConfig