package io.kf.etl.processors.download.transform.utils

import com.trueaccord.scalapb.GeneratedMessageCompanion
import io.kf.etl.common.conf.DataServiceConfig
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl.asyncHttpClient
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods

import scala.annotation.tailrec

case class EntityDataRetriever(config:DataServiceConfig, filters: Seq[String] = Seq.empty[String]) {

  lazy val asyncClient = getAsyncClient()

  lazy val filterQueryString = filters.mkString("&")

  private lazy val scalaPbJson4sParser = new com.trueaccord.scalapb.json.Parser(preservingProtoFieldNames = true)

  @tailrec
  final def retrieve[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]]
    ( entityEndpoint:Option[String], data: Seq[T] = Seq.empty[T])
    ( implicit
        cmp: GeneratedMessageCompanion[T],
        extractor: EntityParentIDExtractor[T]
    ): Seq[T] = {

    def extractEntity(entityJson: JValue): T = {
      extractor.extract(
        scalaPbJson4sParser.fromJsonString[T](JsonMethods.compact(entityJson)),
        entityJson
      )
    }

    entityEndpoint match {
      case None => data // No endpoint means do nothing, return the dataset provided
      case Some(endpoint) => {

        val url = s"${config.url}${endpoint}&limit=100&${filterQueryString}"
        println(s"Retrieving: ${url}")

        val request = asyncClient.prepareGet(url)
        val responseBody = JsonMethods.parse( request.execute.get.getResponseBody )

        val currentDataset =
          (
            responseBody \ "results" match {
              case JNull | JNothing => Seq.empty
              case entity: JObject => Seq(extractEntity(entity))
              case JArray(entities) => entities.map(extractEntity)
            }
          ) ++ data

        // Retrieve content from "next" URL in links, or return our dataset
        responseBody \ "_links" \ "next" match {
          case JNull | JNothing => currentDataset
          case JString(next) => retrieve(Some(s"${next}"), currentDataset)
        }
      }//end of case Some(entities)
    }
  }

  private def getAsyncClient(): AsyncHttpClient = {
    asyncHttpClient
  }

  def stop():Unit = {
    asyncClient.close()
  }
}
