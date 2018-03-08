package io.kf.etl.context

import java.net.{InetAddress, URL}

import com.typesafe.config.{Config, ConfigFactory}
import io.kf.etl.common.Constants._
import io.kf.etl.common.conf.{ESConfig, KFConfig, PostgresqlConfig}
import io.kf.etl.common.context.ContextTrait
import io.kf.etl.common.url.ClasspathURLEnabler
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.collection.convert.WrapAsScala
import scala.util.{Failure, Success, Try}

object Context extends ContextTrait with ClasspathURLEnabler{
  lazy val (hdfs, rootPath) = getHDFS()
  lazy val sparkSession = getSparkSession()
  lazy val postgresql = getPostgresql()
  lazy val esClient = getESClient()

  override def loadConfig(): KFConfig = {

    KFConfig(
      (Option( System.getProperty(CONFIG_FILE_URL) ) match {
        case Some(path) => ConfigFactory.parseURL(new URL(path))
        case None => ConfigFactory.parseURL(new URL(s"classpath:///${DEFAULT_CONFIG_FILE_NAME}"))
      }).resolve()
    )
  }

  private def getESClient(): TransportClient = {

    (new PreBuiltTransportClient(
      Settings.builder()
        .put("cluster.name", config.esConfig.cluster_name)
        .build()
    )).addTransportAddress(new TransportAddress(InetAddress.getByName(config.esConfig.host), config.esConfig.transport_port))
  }

  private def getHDFS(): (FileSystem, String) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", config.hdfsConfig.fs)
    (FileSystem.get(conf), config.hdfsConfig.root)
  }

  private def getSparkSession(): SparkSession = {

    esClient // elasticsearch transport client should be created before SparkSesion

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
//          .config("spark.driver.host", "localhost")
        .getOrCreate()

    }).get

  }

  private def getPostgresql(): PostgresqlConfig = {
    config.postgresqlConfig
  }
}
