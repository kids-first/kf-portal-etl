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

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val wsClient: StandaloneAhcWSClient = StandaloneAhcWSClient()
  private val configFileEnv: Option[String] = sys.env.get(CONFIG_FILE_URL)
  private val configUrl = configFileEnv.map(c => new File(c))
  implicit val config: Config = configUrl.map(f => ConfigFactory.parseFile(f)).getOrElse(ConfigFactory.parseResources("/kf_etl.conf"))
  implicit val esClient: TransportClient = initESClient() // elasticsearch transport client should be created before SparkSession
  implicit val spark: SparkSession = initSparkSession()


  private def initESClient(): TransportClient = {

    System.setProperty("es.set.netty.runtime.available.processors", "false")

    new PreBuiltTransportClient(
      Settings.builder()
        .put("cluster.name", config.getString(CONFIG_NAME_ES_CLUSTER_NAME))
        .build()
    ).addTransportAddress(
      new TransportAddress(InetAddress.getByName(config.getString(CONFIG_NAME_ES_HOST)), config.getInt(CONFIG_NAME_ES_TRANSPORT_PORT))

    )
  }

  private def initSparkSession(): SparkSession = {
    val session = SparkSession.builder().appName("Kids First Portal ETL")

    session
      .config("es.nodes", s"${config.getString(CONFIG_NAME_ES_HOST)}:${config.getInt(CONFIG_NAME_ES_HTTP_PORT)}")
      .config("es.nodes.wan.only", Option(config.getBoolean(CONFIG_NAME_ES_NODES_WAN_ONLY)).getOrElse(false))
      .getOrCreate()

  }


  def close(): Unit = {
    println("Close default context")
    wsClient.close()
    system.terminate()
  }

  sys.addShutdownHook(close())
}
