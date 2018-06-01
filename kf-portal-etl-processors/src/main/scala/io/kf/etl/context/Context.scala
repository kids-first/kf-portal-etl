package io.kf.etl.context

import java.net.{InetAddress, URL}

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.typesafe.config.ConfigFactory
import io.kf.etl.common.Constants._
import io.kf.etl.common.conf._
import io.kf.etl.common.context.ContextTrait
import io.kf.etl.common.url.KfURLEnabler
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

object Context extends ContextTrait with KfURLEnabler{
  lazy val hdfs = getHDFS()
  lazy val rootPath = getRootPath()
  lazy val sparkSession = getSparkSession()
  lazy val mysql = getMysql()
  lazy val esClient = getESClient()
  lazy val dataService = getDataService()
  lazy val awsS3 = getAWS()

  override def loadConfig(): KFConfig = {

    KFConfig(
      (Option( System.getProperty(CONFIG_FILE_URL) ) match {
        case Some(path) => ConfigFactory.parseURL(new URL(path))
        case None => ConfigFactory.parseURL(new URL(s"classpath:///${DEFAULT_CONFIG_FILE_NAME}"))
      }).resolve()
    )
  }

  private def getDataService(): DataServiceConfig = {
    config.dataServiceConfig
  }

  private def getESClient(): TransportClient = {

    System.setProperty("es.set.netty.runtime.available.processors", "false")

    (new PreBuiltTransportClient(
      Settings.builder()
        .put("cluster.name", config.esConfig.cluster_name)
        .build()
    )).addTransportAddress(new TransportAddress(InetAddress.getByName(config.esConfig.host), config.esConfig.transport_port))
  }

  private def getHDFS(): FileSystem = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", config.hdfsConfig.fs)
    FileSystem.get(conf)
  }

  private def getRootPath():String = {
    config.hdfsConfig.root
  }

  private def getSparkSession(): SparkSession = {

    esClient // elasticsearch transport client should be created before SparkSesion

    Some(SparkSession.builder()).map(session => {
      config.sparkConfig.master match {
        case Some(master) => session.master(master)
        case None => session
      }
    }).map(session => {

      Seq(
        config.esConfig.configs,
        config.sparkConfig.properties
      ).foreach(entries => {
        entries.foreach(entry => {
          session.config(
            entry._1.substring(entry._1.indexOf("\"") + 1, entry._1.lastIndexOf("\"")) , // remove starting and ending double quotes
            entry._2)
        })
      })

      session
        .config("es.nodes.wan.only", "true")
        .config("es.nodes", s"${config.esConfig.host}:${config.esConfig.http_port}")
        .getOrCreate()

    }).get

  }

  private def getMysql(): MysqlConfig = {
    config.mysqlConfig
  }

  private def getAWS(): AmazonS3 = {
    AmazonS3ClientBuilder.standard().withCredentials(new ProfileCredentialsProvider(config.awsConfig.s3.profile)).build()
  }
}
