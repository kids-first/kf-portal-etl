package io.kf.etl.context

import com.amazonaws.services.s3.AmazonS3
import io.kf.etl.common.conf.{DataServiceConfig, MysqlConfig}
import io.kf.etl.common.context.ContextBase
import io.kf.etl.common.url.KfURLEnabler
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.elasticsearch.client.transport.TransportClient

trait Context extends ContextBase with KfURLEnabler{
  lazy val hdfs: FileSystem = getHDFS()
  lazy val rootPath: String = getRootPath()
  lazy val mysql: MysqlConfig = getMysql()
  lazy val esClient: TransportClient = getESClient()
  lazy val dataService: DataServiceConfig = getDataService()
  lazy val awsS3: AmazonS3 = getAWS()
  lazy val sparkSession: SparkSession = getSparkSession()


  def getDataService(): DataServiceConfig
  def getESClient(): TransportClient
  def getHDFS(): FileSystem
  def getRootPath():String
  def getMysql(): MysqlConfig
  def getAWS(): AmazonS3
  def getSparkSession(): SparkSession
}
