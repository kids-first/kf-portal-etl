package io.kf.etl.processors.download.source

import org.asynchttpclient.AsyncHttpClient
import org.json4s.JsonAST._
import org.asynchttpclient.Dsl._
import org.json4s.jackson.JsonMethods


case class EntityDataRetrieval(rootUrl:String) {

  private lazy val asyncClient = getAsyncClient()

  def retrieve(entityPath: Option[String]): Seq[JValue] = {
    entityPath match {
      case None => Seq.empty
      case Some(path) => {
        val responseBody = JsonMethods.parse( asyncClient.prepareGet(s"${rootUrl}${path}").execute().get().getResponseBody )

        val currentDataset =
          responseBody \ "results" match {
            case JNull || JNothing => Seq.empty
            case JArray(list) => {
              list.map(jvalue => {
                jvalue.removeField{
                  case JField("_links", _) => true
                  case _ => false
                }
              })
            }
            case _ => Seq.empty
          }

        responseBody \ "_links" \ "next" match {
          case JNull || JNothing => currentDataset
          case JString(next) => currentDataset ++ retrieve(Some(next))
        }
      }
    }
  }

  private def getAsyncClient(): AsyncHttpClient = {
    asyncHttpClient
  }
}
