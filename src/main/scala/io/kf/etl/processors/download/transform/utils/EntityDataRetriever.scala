package io.kf.etl.processors.download.transform.utils

import akka.actor.{ActorSystem, Scheduler}
import akka.pattern.retry
import org.json4s
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods
import play.api.libs.ws.StandaloneWSClient

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class EntityDataRetriever(config: DataServiceConfig, filters: Seq[String] = Seq.empty[String])(implicit wsClient: StandaloneWSClient, ec: ExecutionContext, system: ActorSystem) {

  implicit val scheduler: Scheduler = system.scheduler

  private lazy val filterQueryString = filters.mkString("&")

  final def retrieve[T](endpoint: String, data: Seq[T] = Seq.empty[T])(implicit extractor: EntityDataExtractor[T]): Future[Seq[T]] = {
    def extractDataset(responseBody: json4s.JValue) = {
      responseBody \ "results" match {
        case JNull | JNothing => Seq.empty
        case obj: JObject =>
          val entity = extractEntity(obj)
          entity match {
            case Some(e) => Seq(e)
            case None => Seq.empty
          }
        case JArray(entities) => extractEntityArray(entities)
        case _ => Seq.empty
      }
    }

    def extractEntityArray(entities: List[JValue]): Seq[T] = {
      entities.map(extractEntity).filter(_.isDefined).map(_.get)
    }

    def extractEntity(entityJson: JValue): Option[T] = {
        val entity = extractor.extract(entityJson)
        Some(entity)
    }

    def attempt(nextEndPoint: String): Future[Seq[T]] = {

        val url = s"${config.url}$nextEndPoint&limit=100&$filterQueryString"

        wsClient.url(url).get().flatMap{ response =>
          response.status match {
            case 200 =>
              import play.api.libs.ws.DefaultBodyReadables.readableAsString
              val responseBody = JsonMethods.parse(response.body)
              val currentDataset = data ++ extractDataset(responseBody)
              responseBody \ "_links" \ "next" match {
                case JString(next) => attempt(next)
                case _ => Future.successful(currentDataset)
              }
            case _ => Future.failed(new IllegalStateException(s"Impossible to fetch data from $url, got statusCode=${response.status} and body=${response.body}"))
          }
        }
    }

    retry(() => attempt(endpoint), 10, 100 milliseconds)
  }


}
