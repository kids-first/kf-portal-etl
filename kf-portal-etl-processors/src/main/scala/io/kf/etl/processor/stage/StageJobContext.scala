package io.kf.etl.processor.stage

import java.net.URL

import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import io.kf.etl.common.Constants._

import scala.util.{Failure, Success, Try}

case class StageJobContext(val sparkSession: SparkSession, val fs:FileSystem, val root_path:URL, val config: Option[Config]){
  def getRelativePath():String = {
    config match {
      case Some(cc) => {
        Try(cc.getString(CONFIG_NAME_RELATIVE_PATH)) match {
          case Success(path) => s"/${path}"
          case Failure(_) => STAGE_DEFAULT_RELATIVE_PATH
        }
      }
      case None => STAGE_DEFAULT_RELATIVE_PATH
    }
  }
}
