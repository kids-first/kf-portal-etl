package io.kf.etl.context

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import io.kf.etl.common.Constants._
import io.kf.etl.context.DefaultContext.elasticSearchUrl
import org.apache.spark.sql.SparkSession
import play.api.libs.ws.ahc.StandaloneAhcWSClient

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
    val configFileEnv: Option[String] = sys.env.get(CONFIG_FILE_URL)
    val configUrl = configFileEnv.map(c => new File(c))
    configMutable = configUrl.map(f => ConfigFactory.parseFile(f)).getOrElse(ConfigFactory.parseResources("/kf_etl.conf"))
    sparkMutable = initSparkSession()
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
  def withContext[T](withES: Boolean)(f: DefaultContext => T): T = {
    val context = new DefaultContext()
    try {
      context.init(withES)
      f(context)
    } finally {
      context.close()
    }
  }

  def withContext[T](f: DefaultContext => T): T = withContext(withES = true)(f)

  def elasticSearchUrl(config : Config): String = s"${config.getString(CONFIG_NAME_ES_SCHEME)}://${config.getString(CONFIG_NAME_ES_HOST)}:${config.getInt(CONFIG_NAME_ES_PORT)}"
}
