package io.kf.etl.processors.download.transform

import io.kf.etl.external.dataservice.entity._
import org.json4s.JsonAST.{JNothing, JNull, JString, JValue}

trait EntityParentIDExtractor[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]] {
  def extract(entity:T, json:JValue): T

  def getIdFromLink(linkName: String, json: JValue): Option[String] = {
    json \ "_links" \ linkName match {
      case JNull | JNothing => None
      case JString(endpoint) => Some(endpoint.substring(endpoint.lastIndexOf('/') + 1) )
    }
  }
}

object EntityParentIDExtractor {

  implicit val participant:EntityParentIDExtractor[EParticipant] = new EntityParentIDExtractor[EParticipant] {
    override def extract(entity: EParticipant, json: JValue): EParticipant = {
      entity.copy(
        studyId = getIdFromLink("study", json),
        familyId = getIdFromLink("family", json)
      )
    }
  }

  implicit val family:EntityParentIDExtractor[EFamily] = new EntityParentIDExtractor[EFamily] {
    override def extract(entity: EFamily, json: JValue): EFamily = entity
  }

  implicit val biospecimen:EntityParentIDExtractor[EBiospecimen] = new EntityParentIDExtractor[EBiospecimen] {
    override def extract(entity: EBiospecimen, json: JValue): EBiospecimen = {
      entity.copy(
        participantId = getIdFromLink("participant", json)
      )
    }
  }

  implicit val diagnosis:EntityParentIDExtractor[EDiagnosis] = new EntityParentIDExtractor[EDiagnosis] {
    override def extract(entity: EDiagnosis, json: JValue): EDiagnosis = {
      entity.copy(
        participantId = getIdFromLink("participant", json)
      )
    }
  }

  implicit val familyRelationship:EntityParentIDExtractor[EFamilyRelationship] = new EntityParentIDExtractor[EFamilyRelationship] {
    override def extract(entity: EFamilyRelationship, json: JValue): EFamilyRelationship = {
      entity.copy(
        participant1 = getIdFromLink("participant1", json),
        participant2 = getIdFromLink("participant2", json)
      )
    }
  }

  implicit val genomicFile: EntityParentIDExtractor[EGenomicFile] = new EntityParentIDExtractor[EGenomicFile] {
    override def extract(entity: EGenomicFile, json: JValue): EGenomicFile = {
      entity.copy(
        biospecimenId = getIdFromLink("biospecimen", json),
        sequencingExperimentId = getIdFromLink("sequencing_experiment", json)
      )
    }
  }

  implicit val investigator: EntityParentIDExtractor[EInvestigator] = new EntityParentIDExtractor[EInvestigator] {
    override def extract(entity: EInvestigator, json: JValue): EInvestigator = entity
  }

  implicit val outcome: EntityParentIDExtractor[EOutcome] = new EntityParentIDExtractor[EOutcome] {
    override def extract(entity: EOutcome, json: JValue): EOutcome = {
      entity.copy(
        participantId = getIdFromLink("participant", json)
      )
    }
  }

  implicit val phenotype: EntityParentIDExtractor[EPhenotype] = new EntityParentIDExtractor[EPhenotype] {
    override def extract(entity: EPhenotype, json: JValue): EPhenotype = {
      entity.copy(
        participantId = getIdFromLink("participant", json)
      )
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
      entity.copy(
        studyId = getIdFromLink("study", json)
      )
    }
  }
}
