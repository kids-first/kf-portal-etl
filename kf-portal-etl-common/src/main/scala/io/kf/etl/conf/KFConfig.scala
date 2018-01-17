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
  lazy val postgresqlConfig = getPostgresql()

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
      config.getString(CONFIG_NAME_HDFS_FS),
      config.getString(CONFIG_NAME_HDFS_PATH)
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

  private def getPostgresql(): PostgresqlConfig = {
    PostgresqlConfig(
      config.getString(CONFIG_NAME_POSTGRESQL_HOST),
      config.getString(cONFIG_NAME_POSTGRESQL_DATABASE),
      config.getString(CONFIG_NAME_POSTGRESQL_USER),
      config.getString(CONFIG_NAME_POSTGRESQL_PASSWORD)
    )
  }

}

object KFConfig{
  def apply(config: Config): KFConfig = {
    new KFConfig(config)
  }
}

case class SparkConfig(appName:String, master:String)

case class HDFSConfig(fs:String, root_path:String)

case class ESConfig(url:String, index:String)

case class PostgresqlConfig(host:String, database:String, user:String, password:String)