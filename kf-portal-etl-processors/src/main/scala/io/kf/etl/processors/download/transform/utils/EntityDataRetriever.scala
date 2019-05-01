package io.kf.etl.processors.download.transform.utils

import com.trueaccord.scalapb.json.JsonFormatException
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import io.kf.etl.common.conf.DataServiceConfig
import org.json4s
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods
import play.api.libs.ws.StandaloneWSClient

import scala.concurrent.{ExecutionContext, Future}

case class EntityDataRetriever(config: DataServiceConfig, filters: Seq[String] = Seq.empty[String])(implicit wsClient: StandaloneWSClient, ec: ExecutionContext) {

  private lazy val filterQueryString = filters.mkString("&")

  private lazy val scalaPbJson4sParser = new com.trueaccord.scalapb.json.Parser(preservingProtoFieldNames = true)

  final def retrieve[T <: GeneratedMessage with Message[T]](endpoint: String, data: Seq[T] = Seq.empty[T], retries: Int = 10)(implicit cmp: GeneratedMessageCompanion[T], extractor: EntityParentIDExtractor[T]): Future[Seq[T]] = {
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

      try {
        val entity = extractor.extract(
          scalaPbJson4sParser.fromJsonString[T](JsonMethods.compact(entityJson)),
          entityJson
        )
        Some(entity)
      } catch {
        case _: JsonFormatException => None
        case e: Exception => throw e
      }
    }

    val url = s"${config.url}$endpoint&limit=100&$filterQueryString"
    println(s"Retrieving: $url")

    wsClient.url(url).get().flatMap { response =>
      if (response.status != 200) {
        if (retries > 0)
          retrieve(endpoint, data, retries - 1)
        else
          Future.failed(new IllegalStateException(s"Impossible to fetch data from $url, got statusCode = ${response.status} and body = ${response.body}"))
      } else {
        import play.api.libs.ws.DefaultBodyReadables.readableAsString
        val responseBody = JsonMethods.parse(response.body)
        val currentDataset = data ++ extractDataset(responseBody)
        // Retrieve content from "next" URL in links, or return our dataset
        responseBody \ "_links" \ "next" match {
          case JString(next) => retrieve(next, currentDataset)
          case _ => Future.successful(currentDataset)
        }

      }
    }


  }


}
