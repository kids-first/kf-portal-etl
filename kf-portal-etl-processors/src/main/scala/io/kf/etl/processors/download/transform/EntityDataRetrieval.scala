package io.kf.etl.processors.download.transform

import com.trueaccord.scalapb.GeneratedMessageCompanion
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods


case class EntityDataRetrieval(rootUrl:String) {

  private lazy val asyncClient = getAsyncClient()

  private lazy val scalaPbJson4sParser = new com.trueaccord.scalapb.json.Parser(preservingProtoFieldNames = true)

  def retrieve[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]](entityEndpoint:Option[String])(implicit cmp: GeneratedMessageCompanion[T]): Seq[T] = {
    entityEndpoint match {
      case None => Seq.empty
      case Some(endpoint) => {
        val responseBody = JsonMethods.parse( asyncClient.prepareGet(s"${rootUrl}${endpoint}").execute().get().getResponseBody )

        val currentDataset =
          responseBody \ "results" match {
            case JNull | JNothing => Seq.empty
            case JArray(entities) => {
              entities.map(entity => {
                scalaPbJson4sParser.fromJson[T](
                  entity.removeField{
                    case JField("_links", _) => true
                    case _ => false
                  }
                )
              })
            }//end of case JArray(entities)
          }//end of responseBody \ "results" match

        responseBody \ "_links" \ "next" match {
          case JNull | JNothing => currentDataset
          case JString(next) => currentDataset ++ retrieve(Some(next))
        }
      }//end of case Some(entities)
    }
  }

  private def getAsyncClient(): AsyncHttpClient = {
    asyncHttpClient
  }
}
