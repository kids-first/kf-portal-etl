package io.kf.etl.context

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import io.kf.etl.common.Constants._
import io.kf.etl.common.Utils.getOptionalConfig
import io.kf.etl.context.DefaultContext.elasticSearchUrl
import org.apache.spark.sql.SparkSession
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import java.io.File

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

  private def init(withES: Boolean): Unit = {
    system = ActorSystem()
    materializer = ActorMaterializer()(system)
    wsClientMutable = StandaloneAhcWSClient()(materializer)
    configMutable = initConfig()
    sparkMutable = initSparkSession(withES)
  }

  private def initConfig(): Config = {
    val configFileEnv: Option[String] = sys.env.get(CONFIG_FILE_URL)
    val configUrl = configFileEnv.map(c => new File(c))
    val configFromFiles = configUrl
      .map(f => ConfigFactory.parseFile(f)).getOrElse(ConfigFactory.parseResources("/kf_etl.conf"))
    ConfigFactory.systemEnvironment().withFallback(configFromFiles)
  }

  private def initSparkSession(withES: Boolean): SparkSession = {
    val session = SparkSession.builder().appName("Kids First Portal ETL")

    val user = getOptionalConfig(CONFIG_NAME_ES_USER, configMutable)
    val pwd = getOptionalConfig(CONFIG_NAME_ES_PASS, configMutable)

    if (withES) {
      if (user.isDefined && pwd.isDefined) {
        session
          .config("es.nodes", elasticSearchUrl(configMutable))
          .config("es.nodes.wan.only", Option(configMutable.getBoolean(CONFIG_NAME_ES_NODES_WAN_ONLY)).getOrElse(false))
          .config("es.net.http.auth.user", user.get)
          .config("es.net.http.auth.pass", pwd.get)
          .getOrCreate()
      } else {
        session
          .config("es.nodes", elasticSearchUrl(configMutable))
          .config("es.nodes.wan.only", Option(configMutable.getBoolean(CONFIG_NAME_ES_NODES_WAN_ONLY)).getOrElse(false))
          .getOrCreate()
      }
    } else {
      session.getOrCreate()
    }
  }


  def close(): Unit = {
    println("Close default context")
    wsClientMutable.close()
    system.terminate()
  }

  sys.addShutdownHook(close())
}

object DefaultContext {
  def withContext[T](withES: Boolean = true)(f: DefaultContext => T): T = {
    val context = new DefaultContext()
    try {
      context.init(withES)
      f(context)
    } finally {
      context.close()
    }
  }

  def elasticSearchUrl(config: Config): String = s"${config.getString(CONFIG_NAME_ES_SCHEME)}://${config.getString(CONFIG_NAME_ES_HOST)}:${config.getInt(CONFIG_NAME_ES_PORT)}"
}
