package io.kf.etl.context

import java.io.File
import java.net.InetAddress

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import io.kf.etl.common.Constants._
import org.apache.spark.sql.SparkSession
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import play.api.libs.ws.ahc.StandaloneAhcWSClient

class DefaultContext extends AutoCloseable {
  private var system: ActorSystem = _
  private var materializer: ActorMaterializer = _
  private var wsClientMutable: StandaloneAhcWSClient = _
  private var configMutable: Config = _
  private var esClientMutable: TransportClient = _
  private var sparkMutable: SparkSession = _

  object implicits {
    implicit def wsClient: StandaloneAhcWSClient = wsClientMutable

    implicit def config: Config = configMutable

    implicit def esClient: TransportClient = esClientMutable

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
    if (withES)
      esClientMutable = initESClient() // elasticsearch transport client should be created before SparkSession
    sparkMutable = initSparkSession()
  }

  private def initESClient(): TransportClient = {

    System.setProperty("es.set.netty.runtime.available.processors", "false")

    new PreBuiltTransportClient(
      Settings.builder()
        .put("cluster.name", configMutable.getString(CONFIG_NAME_ES_CLUSTER_NAME))
        .build()
    ).addTransportAddress(
      new TransportAddress(InetAddress.getByName(configMutable.getString(CONFIG_NAME_ES_HOST)), configMutable.getInt(CONFIG_NAME_ES_TRANSPORT_PORT))

    )
  }

  private def initSparkSession(): SparkSession = {
    val session = SparkSession.builder().appName("Kids First Portal ETL")

    session
      .config("es.nodes", s"${configMutable.getString(CONFIG_NAME_ES_HOST)}:${configMutable.getInt(CONFIG_NAME_ES_HTTP_PORT)}")
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
}
