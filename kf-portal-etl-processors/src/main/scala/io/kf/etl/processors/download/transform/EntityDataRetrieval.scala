package io.kf.etl.processors.download.transform

import com.trueaccord.scalapb.GeneratedMessageCompanion
import io.kf.etl.external.dataservice.entity._
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods

import scala.annotation.tailrec

case class EntityDataRetrieval(rootUrl:String) {

  lazy val asyncClient = getAsyncClient()

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
                  scalaPbJson4sParser.fromJsonString[T](JsonMethods.compact(entity)),
                  entity
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


  @tailrec
  final def retrieve1[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]](entityEndpoint:Option[String], data: Seq[T])(implicit cmp: GeneratedMessageCompanion[T], extractor: EntityParentIDExtractor[T]): Seq[T] = {
    entityEndpoint match {
      case None => data
      case Some(endpoint) => {
        val responseBody = JsonMethods.parse( asyncClient.prepareGet(s"${rootUrl}${endpoint}").execute().get().getResponseBody )

        val currentDataset =
          (
            responseBody \ "results" match {
            case JNull | JNothing => Seq.empty
            case entity: JObject => {
              Seq(
                extractor.extract(
                  scalaPbJson4sParser.fromJsonString[T](JsonMethods.compact(entity)),
                  entity
                )
              )
            }
            case JArray(entities) => {
              entities.map(entity => {
                extractor.extract(
                  scalaPbJson4sParser.fromJsonString[T](JsonMethods.compact(entity)),
                  entity
                )
              })
            }//end of case JArray(entities)
          }//end of responseBody \ "results" match
          ) ++ data


        responseBody \ "_links" \ "next" match {
          case JNull | JNothing => retrieve1(None, currentDataset)
          case JString(next) => retrieve1(Some(s"${next}&limit=100"), currentDataset)
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

trait EntityParentIDExtractor[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]] {
  def extract(entity:T, json:JValue): T
}

object EntityParentIDExtractor {

  implicit val participant:EntityParentIDExtractor[EParticipant] = new EntityParentIDExtractor[EParticipant] {
    override def extract(entity: EParticipant, json: JValue): EParticipant = {

      val studyAttached =
        json \ "_links" \ "study" match {
          case JNull | JNothing => entity
          case JString(endpoint) => {
            entity.copy(
              studyId = Some(endpoint.substring(endpoint.lastIndexOf('/') + 1))
            )
          }
        }

      json \ "_links" \ "family" match {
        case JNull | JNothing => studyAttached
        case JString(family) => {
          studyAttached.copy(
            familyId = Some(family.substring(family.lastIndexOf('/') + 1))
          )
        }
      }
    }
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
    override def extract(entity: EFamilyRelationship, json: JValue): EFamilyRelationship = {
      val entity1 =
        json \ "_links" \ "participant" match {
          case JNull | JNothing => entity
          case JString(endpoint) => {
            entity.copy(
              participantId = Some(endpoint.substring(endpoint.lastIndexOf('/') + 1))
            )
          }
        }

      json \ "_links" \ "relative" match {
        case JNull | JNothing => entity1
        case JString(endpoint) => {
          entity1.copy(
            relativeId = Some(endpoint.substring(endpoint.lastIndexOf('/') + 1))
          )
        }
      }
    }
  }

  implicit val genomicFile: EntityParentIDExtractor[EGenomicFile] = new EntityParentIDExtractor[EGenomicFile] {
    override def extract(entity: EGenomicFile, json: JValue): EGenomicFile = {

      val entityWithBio =
        json \ "_links" \ "biospecimen" match {
          case JNull | JNothing => entity
          case JString(endpoint) => {
            entity.copy(
              biospecimenId = Some(endpoint.substring(endpoint.lastIndexOf('/') + 1))
            )
          }
        }

      json \ "_links" \ "sequencing_experiment" match {
        case JNull | JNothing => entityWithBio
        case JString(endpoint) => {
          entityWithBio.copy(
            sequencingExperimentId = Some(endpoint.substring(endpoint.lastIndexOf('/') + 1))
          )
        }
      }
    }
  }

  implicit val investigator: EntityParentIDExtractor[EInvestigator] = new EntityParentIDExtractor[EInvestigator] {
    override def extract(entity: EInvestigator, json: JValue): EInvestigator = entity
  }

  implicit val outcome: EntityParentIDExtractor[EOutcome] = new EntityParentIDExtractor[EOutcome] {
    override def extract(entity: EOutcome, json: JValue): EOutcome = {
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

  implicit val phenotype: EntityParentIDExtractor[EPhenotype] = new EntityParentIDExtractor[EPhenotype] {
    override def extract(entity: EPhenotype, json: JValue): EPhenotype = {
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

  implicit val seqExp: EntityParentIDExtractor[ESequencingExperiment] = new EntityParentIDExtractor[ESequencingExperiment] {
    override def extract(entity: ESequencingExperiment, json: JValue): ESequencingExperiment = entity
  }

  implicit val study: EntityParentIDExtractor[EStudy] = new EntityParentIDExtractor[EStudy] {
    override def extract(entity: EStudy, json: JValue): EStudy = entity
  }

  implicit val studyFile: EntityParentIDExtractor[EStudyFile] = new EntityParentIDExtractor[EStudyFile] {
    override def extract(entity: EStudyFile, json: JValue): EStudyFile = {
      json \ "_links" \ "study" match {
        case JNull | JNothing => entity
        case JString(endpoint) => {
          entity.copy(
            studyId = Some(endpoint.substring(endpoint.lastIndexOf('/') + 1))
          )
        }
      }
    }
  }






}