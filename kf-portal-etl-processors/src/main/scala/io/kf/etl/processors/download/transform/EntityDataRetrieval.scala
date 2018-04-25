package io.kf.etl.processors.download.transform

import com.trueaccord.scalapb.GeneratedMessageCompanion
import io.kf.etl.external.dataservice.entity._
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods

case class EntityDataRetrieval(rootUrl:String) {

  private lazy val asyncClient = getAsyncClient()

  private lazy val scalaPbJson4sParser = new com.trueaccord.scalapb.json.Parser(preservingProtoFieldNames = true)

  def retrieve[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]](entityEndpoint:Option[String])(implicit cmp: GeneratedMessageCompanion[T], extractor: EntityParentIDExtractor[T]): Seq[T] = {
    entityEndpoint match {
      case None => Seq.empty
      case Some(endpoint) => {
        val responseBody = JsonMethods.parse( asyncClient.prepareGet(s"${rootUrl}${endpoint}").execute().get().getResponseBody )

        val currentDataset =
          responseBody \ "results" match {
            case JNull | JNothing => Seq.empty
            case JArray(entities) => {
              entities.map(entity => {
                extractor.extract(
                  scalaPbJson4sParser.fromJson(entity),
                  entity
                )
//                scalaPbJson4sParser.fromJson[T](
//                  entity.removeField{
//                    case JField("_links", _) => true
//                    case _ => false
//                  }
//                )
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

trait EntityParentIDExtractor[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]] {
  def extract(entity:T, json:JValue): T
}

object EntityParentIDExtractor {

  implicit val participant:EntityParentIDExtractor[EParticipant] = new EntityParentIDExtractor[EParticipant] {
    override def extract(entity: EParticipant, json: JValue): EParticipant = entity
  }

  implicit val family:EntityParentIDExtractor[EFamily] = new EntityParentIDExtractor[EFamily] {
    override def extract(entity: EFamily, json: JValue): EFamily = entity
  }

  implicit val biospecimen:EntityParentIDExtractor[EBiospecimen] = new EntityParentIDExtractor[EBiospecimen] {
    override def extract(entity: EBiospecimen, json: JValue): EBiospecimen = {
      json \ "_links" \ "participant" match {
        case JNull | JNothing => entity
        case JString(endpoint) => {
          entity.copy(
            participantId = Some(endpoint.substring(endpoint.lastIndexOf('/') + 1))
          )
        }
      }
    }
  }

  implicit val diagnosis:EntityParentIDExtractor[EDiagnosis] = new EntityParentIDExtractor[EDiagnosis] {
    override def extract(entity: EDiagnosis, json: JValue): EDiagnosis = {
      json \ "_links" \ "participant" match {
        case JNull | JNothing => entity
        case JString(endpoint) => {
          entity.copy(
            participantId = Some(endpoint.substring(endpoint.lastIndexOf('/') + 1))
          )
        }
      }
    }
  }

  implicit val familyRelationship:EntityParentIDExtractor[EFamilyRelationship] = new EntityParentIDExtractor[EFamilyRelationship] {
    override def extract(entity: EFamilyRelationship, json: JValue): EFamilyRelationship = entity
  }

  implicit val genomicFile: EntityParentIDExtractor[EGenomicFile] = new EntityParentIDExtractor[EGenomicFile] {
    override def extract(entity: EGenomicFile, json: JValue): EGenomicFile = entity
  }

  implicit val investigator: EntityParentIDExtractor[EInvestigator] = new EntityParentIDExtractor[EInvestigator] {
    override def extract(entity: EInvestigator, json: JValue): EInvestigator = entity
  }

  implicit val outcome: EntityParentIDExtractor[EOutcome] = new EntityParentIDExtractor[EOutcome] {
    override def extract(entity: EOutcome, json: JValue): EOutcome = entity
  }

  implicit val phenotype: EntityParentIDExtractor[EPhenotype] = new EntityParentIDExtractor[EPhenotype] {
    override def extract(entity: EPhenotype, json: JValue): EPhenotype = entity
  }

  implicit val seqExp: EntityParentIDExtractor[ESequencingExperiment] = new EntityParentIDExtractor[ESequencingExperiment] {
    override def extract(entity: ESequencingExperiment, json: JValue): ESequencingExperiment = entity
  }

  implicit val study: EntityParentIDExtractor[EStudy] = new EntityParentIDExtractor[EStudy] {
    override def extract(entity: EStudy, json: JValue): EStudy = entity
  }

  implicit val studyFile: EntityParentIDExtractor[EStudyFile] = new EntityParentIDExtractor[EStudyFile] {
    override def extract(entity: EStudyFile, json: JValue): EStudyFile = entity
  }






}