package io.kf.etl.processors.download.transform.utils

import com.trueaccord.scalapb.GeneratedMessageCompanion
import com.trueaccord.scalapb.json.JsonFormatException
import io.kf.etl.common.conf.DataServiceConfig
import org.asynchttpclient.Dsl.asyncHttpClient
import org.asynchttpclient.{AsyncHttpClient, Response}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods

import scala.annotation.tailrec

case class EntityDataRetriever(config: DataServiceConfig, filters: Seq[String] = Seq.empty[String]) {

  private lazy val asyncClient = getAsyncClient

  private lazy val filterQueryString = filters.mkString("&")

  private lazy val scalaPbJson4sParser = new com.trueaccord.scalapb.json.Parser(preservingProtoFieldNames = true)


  @tailrec
  final def retrieve[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]]
  (entityEndpoint: Option[String], data: Seq[T] = Seq.empty[T], retries: Int = 10)
  (implicit
   cmp: GeneratedMessageCompanion[T],
   extractor: EntityParentIDExtractor[T]
  ): Seq[T] = {

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

    entityEndpoint match {
      case None => data // No endpoint means do nothing, return the dataset provided
      case Some(endpoint) =>

        val url = s"${config.url}$endpoint&limit=100&$filterQueryString"
        println(s"Retrieving: $url")

        val request = asyncClient.prepareGet(url)
        val response: Response = request.execute.get
        if (response.getStatusCode != 200) {
          if (retries > 0)
            retrieve(entityEndpoint, data, retries - 1)
          else
            throw new IllegalStateException("error endpoint")
        } else {
          val responseBody = JsonMethods.parse(response.getResponseBody)

          val currentDataset =
            (
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
              ) ++ data

          // Retrieve content from "next" URL in links, or return our dataset
          responseBody \ "_links" \ "next" match {
            case JNull | JNothing => currentDataset
            case JString(next) => retrieve(Some(s"$next"), currentDataset)
            case _ => currentDataset
          }
        } //end of case Some(entities)
    }
  }

  private def getAsyncClient: AsyncHttpClient = {
    asyncHttpClient
  }

  def stop(): Unit = {
    asyncClient.close()
  }
}
