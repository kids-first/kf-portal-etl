package io.kf.etl.context

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.spark.sql.SparkSession
import play.api.libs.ws.StandaloneWSClient
import play.api.libs.ws.ahc.StandaloneAhcWSClient

class DefaultContext extends ContextCommon {

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  val wsClient: StandaloneAhcWSClient = StandaloneAhcWSClient()

  override def getSparkSession(): SparkSession = {

    esClient // elasticsearch transport client should be created before SparkSession

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
            entry._1.substring(entry._1.indexOf("\"") + 1, entry._1.lastIndexOf("\"")), // remove starting and ending double quotes
            entry._2)
        })
      })

      session
        .config("es.nodes.wan.only", "true")
        .config("es.nodes", s"${config.esConfig.host}:${config.esConfig.http_port}")
        .getOrCreate()

    }).get

  }

  override def getWsClient(): StandaloneWSClient = {
    wsClient
  }

  def close(): Unit = {
    wsClient.close()
    system.terminate()
    getSparkSession().close()
  }

  sys.addShutdownHook(close())
}
