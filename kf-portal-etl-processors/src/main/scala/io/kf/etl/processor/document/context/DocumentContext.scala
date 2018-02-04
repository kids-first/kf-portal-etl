package io.kf.etl.processor.document.context

import com.typesafe.config.Config
import io.kf.etl.common.Constants._
import io.kf.etl.processor.common.ProcessorContext
import org.apache.hadoop.fs.{FileSystem => HDFS}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

case class DocumentContext(override val sparkSession: SparkSession,
                           override val hdfs: HDFS,
                           override val appRootPath: String,
                           override val config: Option[Config]) extends ProcessorContext{
  def getJobDataPath():String = {
    config match {
      case Some(cc) => {
        Try(cc.getString(CONFIG_NAME_DATA_PATH)) match {
          case Success(path) => path
          case Failure(_) => s"${appRootPath}/${DOCUMENT_DEFAULT_DATA_PATH}"
        }
      }
      case None => s"${appRootPath}/${DOCUMENT_DEFAULT_DATA_PATH}"
    }
  }
}
