package io.kf.etl.processor.document.context

import io.kf.etl.common.Constants._
import io.kf.etl.common.conf.PostgresqlConfig
import io.kf.etl.processor.common.{ProcessorConfig, ProcessorContext}
import org.apache.hadoop.fs.{FileSystem => HDFS}
import org.apache.spark.sql.SparkSession


case class DocumentContext(override val sparkSession: SparkSession,
                           override val hdfs: HDFS,
                           override val appRootPath: String,
                           override val config: DocumentConfig) extends ProcessorContext{
  def getJobDataPath():String = {
    config.dataPath match {
      case Some(cc) => cc
      case None => s"${appRootPath}/${DOCUMENT_DEFAULT_DATA_PATH}"
    }
  }
}


case class DocumentConfig(override val name:String, override val dataPath: Option[String], val postgresql: PostgresqlConfig) extends ProcessorConfig