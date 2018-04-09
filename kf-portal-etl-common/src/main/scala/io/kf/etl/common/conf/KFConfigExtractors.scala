package io.kf.etl.common.conf

import com.typesafe.config.Config
import io.kf.etl.common.Constants._

import scala.collection.convert.WrapAsScala
import scala.util.{Failure, Success, Try}

object KFConfigExtractors {
  def parseMySQL(config:Config): MysqlConfig = {
    MysqlConfig(
      config.getString(CONFIG_NAME_MYSQL_HOST),
      config.getString(cONFIG_NAME_MYSQL_DATABASE),
      config.getString(CONFIG_NAME_MYSQL_USER),
      config.getString(CONFIG_NAME_MYSQL_PASSWORD),
      WrapAsScala.asScalaBuffer( config.getStringList(CONFIG_NAME_MYSQL_PROPERTIES) )
    )
  }

  def parsePostgresQL(config:Config):PostgresqlConfig = {
    PostgresqlConfig(
      config.getString(CONFIG_NAME_POSTGRESQL_HOST),
      config.getString(cONFIG_NAME_POSTGRESQL_DATABASE),
      config.getString(CONFIG_NAME_POSTGRESQL_USER),
      config.getString(CONFIG_NAME_POSTGRESQL_PASSWORD)
    )
  }

  def parseElasticsearch(config:Config): ESConfig = {
    ESConfig(
      host = config.getString(CONFIG_NAME_ES_HOST),
      cluster_name = config.getString(CONFIG_NAME_ES_CLUSTER_NAME),
      http_port = Try(config.getInt(CONFIG_NAME_ES_HTTP_PORT)) match {
        case Success(port) => port
        case Failure(_) => 9200
      },
      transport_port = Try(config.getInt(CONFIG_NAME_ES_TRANSPORT_PORT))  match {
        case Success(port) => port
        case Failure(_) => 9300
      },
      configs = {
        Try(config.getConfig(CONFIG_NAME_ES_CONFIGS)) match {
          case Success(config) => {
            WrapAsScala.asScalaSet(config.entrySet()).map(entry => {
              (
                entry.getKey,
                entry.getValue.unwrapped().toString
              )
            }).toMap
          }
          case Failure(_) => Map.empty[String, String]
        }
      }
    )
  }

  def parseSpark(config:Config): SparkConfig = {
    SparkConfig(
      appName = Option(config.getString(CONFIG_NAME_SPARK_APP_NAME)) match {
        case Some(name) => name
        case None => DEFAULT_APP_NAME
      },
      master = Try(config.getString(CONFIG_NAME_SPARK_MASTER)) match {
        case Success(master) => Some(master)
        case _ => None
      },
      properties = {
        Try(config.getConfig(CONFIG_NAME_SPARK_PROPERTIES)) match {
          case Success(config) => {
            WrapAsScala.asScalaSet(config.entrySet()).map(entry => {
              (
                entry.getKey,
                entry.getValue.unwrapped().toString
              )
            }).toMap
          }
          case Failure(_) => Map.empty[String, String]
        }
      }
    )
  }

  def parseHDFS(config:Config): HDFSConfig = {
    HDFSConfig(
      config.getString(CONFIG_NAME_HDFS_FS),
      config.getString(CONFIG_NAME_HDFS_PATH)
    )
  }
}
