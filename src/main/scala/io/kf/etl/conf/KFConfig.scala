package io.kf.etl.conf

import com.typesafe.config.Config
import io.kf.etl.Constants._

import scala.collection.convert.WrapAsScala

class KFConfig(private val config: Config){

  lazy val sparkConfig = getSparkConfig()
  lazy val esConfig = getESConfig()
  lazy val hdfsConfig = getHDFSConfig()
  lazy val processorsConfig = getProcessors()
  lazy val pipelineConfig = getPipeline()

  private def getSparkConfig(): SparkConfig = {
    SparkConfig(
      Option(config.getString(CONFIG_NAME_SPARK_APP_NAME)) match {
        case Some(name) => name
        case None => DEFAULT_APP_NAME
      },
      Option(config.getString(CONFIG_NAME_SPARK_MASTER)) match {
        case Some(master) => master
        case None => "local[*]"
      }
    )
  }

  private def getESConfig(): ESConfig = {
    ESConfig(
      config.getString(CONFIG_NAME_ES_URL),
      config.getString(CONFIG_NAME_ES_INDEX)
    )
  }

  private def getHDFSConfig(): HDFSConfig = {
    HDFSConfig(
      config.getString(CONFIG_NAME_HDFS_DEFAULTFS)
    )
  }

  private def getProcessors(): Map[String, Config] = {
    WrapAsScala.asScalaBuffer( config.getConfigList(CONFIG_NAME_PROCESSORS) ).map(config => {
      (config.getString("name"), config)
    }).toMap
  }

  private def getPipeline(): Config = {
    config.getConfig(CONFIG_NAME_PIPELINE)
  }
}

object KFConfig{
  def apply(config: Config): KFConfig = {
    new KFConfig(config)
  }
}

case class SparkConfig(appName:String, master:String)

case class HDFSConfig(fs:String)

case class ESConfig(url:String, index:String)