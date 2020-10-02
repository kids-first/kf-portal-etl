package io.kf.etl.context

import java.io.File
import java.util.Properties

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import io.kf.etl.common.Constants._
import io.kf.etl.context.DefaultContext.elasticSearchUrl
import org.apache.spark.sql.SparkSession
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.collection.JavaConverters._

class DefaultContext extends AutoCloseable {
  private var system: ActorSystem = _
  private var materializer: ActorMaterializer = _
  private var wsClientMutable: StandaloneAhcWSClient = _
  private var configMutable: Config = _
  private var sparkMutable: SparkSession = _

  object implicits {
    implicit def wsClient: StandaloneAhcWSClient = wsClientMutable

    implicit def config: Config = configMutable

    implicit def spark: SparkSession = sparkMutable

    implicit def actorSystem: ActorSystem = system
  }

  private def init(): Unit = {
    system = ActorSystem()
    materializer = ActorMaterializer()(system)
    wsClientMutable = StandaloneAhcWSClient()(materializer)
    configMutable = initConfig()
    sparkMutable = initSparkSession()
  }

  private def initConfig(): Config = {
    val configFileEnv: Option[String] = sys.env.get(CONFIG_FILE_URL)
    val configUrl = configFileEnv.map(c => new File(c))
    val configFromFiles = configUrl
      .map(f => ConfigFactory.parseFile(f)).getOrElse(ConfigFactory.parseResources("/kf_etl.conf"))
    ConfigFactory.systemEnvironment().withFallback(configFromFiles)
  }

  private def initSparkSession(): SparkSession = {
    val session = SparkSession.builder().appName("Kids First Portal ETL")

    session
      .config("es.nodes", elasticSearchUrl(configMutable))
      .config("es.nodes.wan.only", Option(configMutable.getBoolean(CONFIG_NAME_ES_NODES_WAN_ONLY)).getOrElse(false))
      .getOrCreate()

  }


  def close(): Unit = {
    println("Close default context")
    wsClientMutable.close()
    system.terminate()
  }

  sys.addShutdownHook(close())
}

object DefaultContext {
  def withContext[T](f: DefaultContext => T): T = {
    val context = new DefaultContext()
    try {
      context.init()
      f(context)
    } finally {
      context.close()
    }
  }

  def elasticSearchUrl(config: Config): String = s"${config.getString(CONFIG_NAME_ES_SCHEME)}://${config.getString(CONFIG_NAME_ES_HOST)}:${config.getInt(CONFIG_NAME_ES_PORT)}"
}
