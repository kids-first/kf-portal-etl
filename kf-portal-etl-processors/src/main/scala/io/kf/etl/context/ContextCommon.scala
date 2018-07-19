package io.kf.etl.context

import java.net.{InetAddress, URL}

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.typesafe.config.ConfigFactory
import io.kf.etl.common.Constants.{CONFIG_FILE_URL, DEFAULT_CONFIG_FILE_NAME}
import io.kf.etl.common.conf.{DataServiceConfig, KFConfig, MysqlConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

trait ContextCommon extends Context{

  override def loadConfig(): KFConfig = {

    KFConfig(
      (Option( System.getProperty(CONFIG_FILE_URL) ) match {
        case Some(path) => ConfigFactory.parseURL(new URL(path))
        case None => ConfigFactory.parseURL(new URL(s"classpath:///${DEFAULT_CONFIG_FILE_NAME}"))
      }).resolve()
    )
  }

  override def getDataService(): DataServiceConfig = {
    config.dataServiceConfig
  }

  override def getESClient(): TransportClient = {

    System.setProperty("es.set.netty.runtime.available.processors", "false")

    (new PreBuiltTransportClient(
      Settings.builder()
        .put("cluster.name", config.esConfig.cluster_name)
        .build()
    )).addTransportAddress(new TransportAddress(InetAddress.getByName(config.esConfig.host), config.esConfig.transport_port))
  }

  override def getHDFS(): FileSystem = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", config.hdfsConfig.fs)
    FileSystem.get(conf)
  }

  override def getRootPath():String = {
    config.hdfsConfig.root
  }

  override def getMysql(): MysqlConfig = {
    config.mysqlConfig
  }

  override def getAWS(): Option[AmazonS3] = {

    config.awsConfig match {
      case Some(aws) => Some(
        AmazonS3ClientBuilder.standard().withCredentials(new ProfileCredentialsProvider(aws.s3.profile)).build()
      )
      case None => None
    }

  }
}
