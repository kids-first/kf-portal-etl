package io.kf.etl.context

import java.net.URL

import com.typesafe.config.{Config, ConfigFactory}
import io.kf.etl.common.Constants._
import io.kf.etl.common.conf.{KFConfig, PostgresqlConfig}
import io.kf.etl.common.context.ContextTrait
import io.kf.etl.common.url.ClasspathURLEnabler
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object Context extends ContextTrait with ClasspathURLEnabler{
  lazy val (hdfs, rootPath) = getHDFS()
  lazy val sparkSession = getSparkSession()
  lazy val postgresql = getPostgresql()

  override def loadConfig(): KFConfig = {

    KFConfig(
      (Option( System.getProperty(CONFIG_FILE_URL) ) match {
        case Some(path) => ConfigFactory.parseURL(new URL(path))
        case None => ConfigFactory.parseURL(new URL(s"classpath:///${DEFAULT_CONFIG_FILE_NAME}"))
      }).resolve()
    )
  }

  private def getHDFS(): (FileSystem, String) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", config.hdfsConfig.fs)
    (FileSystem.get(conf), config.hdfsConfig.root)
  }

  private def getSparkSession(): SparkSession = {

    Some(SparkSession.builder()).map(session => {
      config.sparkConfig.master match {
        case Some(master) => session.master(master)
        case None => session
      }
    }).map(session => {

      config.esConfig.configs.map(tuple => {
        session.config(tuple._1, tuple._2)
      })

      session
        .config("es.nodes.wan.only", "true")
        .config("es.nodes", s"${config.esConfig.host}:${config.esConfig.http_port}")
        .getOrCreate()

    }).get

  }

  private def getPostgresql(): PostgresqlConfig = {
    config.postgresqlConfig
  }
}
