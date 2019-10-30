package io.kf.etl.processors.download.transform.utils

import io.kf.etl.models.dataservice._
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JString, JValue}

trait EntityDataExtractor[T] {
  def extract(json: JValue): T

  def getIdFromLink(linkName: String, json: JValue): Option[String] = {
    json \ "_links" \ linkName match {
      case JString(endpoint) => Some(endpoint.substring(endpoint.lastIndexOf('/') + 1))
      case _ => None
    }
  }

}

object EntityDataExtractor {
  private implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val participant: EntityDataExtractor[EParticipant] = new EntityDataExtractor[EParticipant] {
    override def extract(json: JValue): EParticipant = {
      val entity = json.camelizeKeys.extract[EParticipant]
      entity.copy(
        studyId = getIdFromLink("study", json),
        familyId = getIdFromLink("family", json)
      )
    }
  }

  implicit val family: EntityDataExtractor[EFamily] = new EntityDataExtractor[EFamily] {
    override def extract(json: JValue): EFamily = json.camelizeKeys.extract[EFamily]
  }

  implicit val biospecimen: EntityDataExtractor[EBiospecimen] = new EntityDataExtractor[EBiospecimen] {
    override def extract(json: JValue): EBiospecimen = {
      val entity = json.camelizeKeys.extract[EBiospecimen]
      entity.copy(
        participantId = getIdFromLink("participant", json)
      )
    }
  }

  implicit val diagnosis: EntityDataExtractor[EDiagnosis] = new EntityDataExtractor[EDiagnosis] {
    override def extract(json: JValue): EDiagnosis = {
      val entity = json.camelizeKeys.extract[EDiagnosis]
      entity.copy(
        participantId = getIdFromLink("participant", json)
      )
    }
  }

  implicit val familyRelationship: EntityDataExtractor[EFamilyRelationship] = new EntityDataExtractor[EFamilyRelationship] {
    override def extract(json: JValue): EFamilyRelationship = {
      val entity = json.camelizeKeys.extract[EFamilyRelationship]
      entity.copy(
        participant1 = getIdFromLink("participant1", json),
        participant2 = getIdFromLink("participant2", json)
      )
    }
  }

  implicit val genomicFile: EntityDataExtractor[EGenomicFile] = new EntityDataExtractor[EGenomicFile] {
    override def extract(json: JValue): EGenomicFile = json.camelizeKeys.extract[EGenomicFile]
  }

  implicit val biospecimenGenomicFile: EntityDataExtractor[EBiospecimenGenomicFile] = new EntityDataExtractor[EBiospecimenGenomicFile] {
    override def extract(json: JValue): EBiospecimenGenomicFile = {
      val entity = json.camelizeKeys.extract[EBiospecimenGenomicFile]
      entity.copy(
        biospecimenId = getIdFromLink("biospecimen", json),
        genomicFileId = getIdFromLink("genomic_file", json)
      )
    }
  }

  implicit val biospecimeDiagnosis: EntityDataExtractor[EBiospecimenDiagnosis] = new EntityDataExtractor[EBiospecimenDiagnosis] {
    override def extract(json: JValue): EBiospecimenDiagnosis = {
      val entity = json.camelizeKeys.extract[EBiospecimenDiagnosis]
      entity.copy(
        biospecimenId = getIdFromLink("biospecimen", json),
        diagnosisId = getIdFromLink("diagnosis", json)
      )
    }
  }

  implicit val investigator: EntityDataExtractor[EInvestigator] = new EntityDataExtractor[EInvestigator] {
    override def extract(json: JValue): EInvestigator = json.camelizeKeys.extract[EInvestigator]
  }

  implicit val outcome: EntityDataExtractor[EOutcome] = new EntityDataExtractor[EOutcome] {
    override def extract(json: JValue): EOutcome = {
      val entity = json.camelizeKeys.extract[EOutcome]
      entity.copy(
        participantId = getIdFromLink("participant", json)
      )
    }
  }

  implicit val phenotype: EntityDataExtractor[EPhenotype] = new EntityDataExtractor[EPhenotype] {
    override def extract(json: JValue): EPhenotype = {
      val entity = json.camelizeKeys.extract[EPhenotype]
      entity.copy(
        participantId = getIdFromLink("participant", json)
      )
    }
  }

  implicit val seqExp: EntityDataExtractor[ESequencingExperiment] = new EntityDataExtractor[ESequencingExperiment] {
    override def extract(json: JValue): ESequencingExperiment = json.camelizeKeys.extract[ESequencingExperiment]
  }

  implicit val seqExpGenomicFile: EntityDataExtractor[ESequencingExperimentGenomicFile] = new EntityDataExtractor[ESequencingExperimentGenomicFile] {
    override def extract(json: JValue): ESequencingExperimentGenomicFile = {
      val entity = json.camelizeKeys.extract[ESequencingExperimentGenomicFile]
      entity.copy(
        sequencingExperiment = getIdFromLink("sequencing_experiment", json),
        genomicFile = getIdFromLink("genomic_file", json)
      )
    }
  }

  implicit val study: EntityDataExtractor[EStudy] = new EntityDataExtractor[EStudy] {
    override def extract(json: JValue): EStudy = json.camelizeKeys.extract[EStudy]
  }

  implicit val studyFile: EntityDataExtractor[EStudyFile] = new EntityDataExtractor[EStudyFile] {
    override def extract(json: JValue): EStudyFile = {
      val entity = json.camelizeKeys.extract[EStudyFile]
      entity.copy(
        studyId = getIdFromLink("study", json)
      )
    }
  }
}
