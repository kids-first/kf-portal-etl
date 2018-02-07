package io.kf.etl.processor.download.context

import com.typesafe.config.Config
import io.kf.etl.common.Constants._
import io.kf.etl.common.conf.{ESConfig, PostgresqlConfig}
import io.kf.etl.processor.common.{ProcessorConfig, ProcessorContext}
import org.apache.hadoop.fs.{FileSystem => HDFS}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

case class DownloadContext(override val sparkSession: SparkSession,
                            override val hdfs: HDFS,
                           override val appRootPath:String,
                           override val config: DownloadConfig) extends ProcessorContext {
  def getJobDataPath():String = {
    config.dataPath match {
      case Some(cc) => cc
      case None => s"${appRootPath}/${DOWNLOAD_DEFAULT_DATA_PATH}"
    }
  }
}


case class DownloadConfig(override val name:String, postgresql: PostgresqlConfig, dumpPath:String, override val dataPath:Option[String]) extends ProcessorConfig