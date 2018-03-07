package io.kf.etl.processors.index.inject

import java.net.InetAddress

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.Constants._
import io.kf.etl.common.conf.ESConfig
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.context.Context
import io.kf.etl.processors.common.inject.ProcessorInjectModule
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.index.context.{IndexConfig, IndexContext}
import io.kf.etl.processors.index.sink.IndexSink
import io.kf.etl.processors.index.source.IndexSource
import io.kf.etl.processors.index.transform.IndexTransformer
import io.kf.etl.processors.index.transform.releasetag.ReleaseTag
import io.kf.etl.processors.index.transform.releasetag.impl.DateTimeReleaseTag
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem => HDFS}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.collection.convert.WrapAsScala
import scala.util.{Failure, Success, Try}

@GuiceModule(name = "index")
class IndexInjectModule(sparkSession: SparkSession,
                        hdfs: HDFS,
                        appRootPath: String,
                        config: Option[Config]) extends ProcessorInjectModule(sparkSession, hdfs, appRootPath, config) {
  type CONTEXT = IndexContext
  type PROCESSOR = IndexProcessor
  type SOURCE = IndexSource
  type SINK = IndexSink
  type TRANSFORMER = IndexTransformer
  type OUTPUT = Unit

  private val esConfig = parseESConfig()

  private def checkConfig(): Boolean = {
    config.isDefined && (
      Try(config.get.getConfig("elasticsearch")) match {
        case Success(cc) => true
        case Failure(_) => false
      }
    )
  }

  require(checkConfig())

  private def parseESConfig(): ESConfig = {
    val esConfig = config.get.getConfig("elasticsearch")

    ESConfig(
      host = esConfig.getString("host"),
      cluster_name = esConfig.getString("cluster_name"),
      http_port = Try(esConfig.getInt("http_port")) match {
        case Success(port) => port
        case Failure(_) => 9200
      },
      transport_port = Try(esConfig.getInt("transport_port"))  match {
        case Success(port) => port
        case Failure(_) => 9300
      },
      configs = {
        Try(esConfig.getConfig("configs")) match {
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

  override def getContext(): IndexContext = {

    val cc = IndexConfig(
      config.get.getString("name"),
      esConfig,
      None
    )

    new IndexContext(sparkSession, hdfs, appRootPath, cc)
  }

  private def getReleaseTagInstance(): ReleaseTag = {
    Try(config.get.getConfig(RELEASE_TAG)) match {
      case Success(config) => {
        Class.forName(config.getString(RELEASE_TAG_CLASS_NAME))
          .getConstructor(classOf[Map[String, String]])
          .newInstance(
            WrapAsScala.asScalaSet( config.entrySet() ).map(entry => {
              (entry.getKey, entry.getValue.unwrapped().toString)
            }).toMap
          ).asInstanceOf[ReleaseTag]
      }
      case Failure(_) => new DateTimeReleaseTag(Map.empty[String, String])
    }
  }

  @Provides
  override def getProcessor(): IndexProcessor = {
    val context = getContext()
    val source = getSource(context)
    val sink = getSink(context)
    val transformer = getTransformer(context)

    new IndexProcessor(
      context,
      source.source,
      transformer.transform,
      sink.sink
    )
  }

  override def getSource(context: IndexContext): IndexSource = {
    new IndexSource(context)
  }

  override def getSink(context: IndexContext): IndexSink = {

    new IndexSink(
      sparkSession,
      context.config.eSConfig,
      getReleaseTagInstance(),
      Context.esClient
    )
  }

  override def getTransformer(context: IndexContext): IndexTransformer = {
    new IndexTransformer(context)
  }

  override def configure(): Unit = {}

  override def getOutput(context: IndexContext): Unit = {
    Unit
  }
}
