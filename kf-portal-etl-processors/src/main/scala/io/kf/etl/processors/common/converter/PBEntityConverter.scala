package io.kf.etl.processors.common.converter

import io.kf.etl.es.models._
import io.kf.etl.external.dataservice.entity._

object PBEntityConverter {
  
  def EStudyToStudyES(study: EStudy): Study_ES = {
    Study_ES(
      kfId = study.kfId,
      createdAt = study.createdAt,
      modifiedAt = study.modifiedAt,
      attribution = study.attribution,
      name = study.name,
      version = study.version,
      externalId = study.externalId,
      releaseStatus = study.releaseStatus,
      dataAccessAuthority = study.dataAccessAuthority
    )
  }
  
  def EParticipantToParticipantES(participant: EParticipant): Participant_ES = {
    Participant_ES(
      aliasGroup = participant.aliasGroup,
      consentType = participant.consentType,
      createdAt = participant.createdAt,
      ethnicity = participant.ethnicity,
      externalId = participant.externalId,
      familyId = participant.familyId,
      gender = participant.gender,
      isProband = participant.isProband,
      kfId = participant.kfId,
      modifiedAt = participant.modifiedAt,
      race = participant.race,
      studyId = participant.studyId
    )
  }
  
  def EBiospecimenToBiospecimenES(bio: EBiospecimen): Biospecimen_ES = {
    Biospecimen_ES(
      ageAtEventDays = bio.ageAtEventDays,
      analyteType = bio.analyteType,
      anatomicalSite = bio.anatomicalSite,
      composition = bio.composition,
      concentrationMgPerMl = bio.concentrationMgPerMl,
      createdAt = bio.createdAt,
      externalAliquotId = bio.externalAliquotId,
      externalSampleId = bio.externalSampleId,
      kfId = bio.kfId,
      modifiedAt = bio.modifiedAt,
      sequencingCenterId = bio.sequencingCenterId,
      shipmentDate = bio.shipmentDate,
      shipmentOrigin = bio.shipmentOrigin,
      tissueType = bio.tissueType,
      tumorDescriptor = bio.tumorDescriptor,
      uberonId = bio.uberonId,
      volumeMl = bio.volumeMl
    )
  }
  
  def EDiagnosisToDiagnosisES(diagnosis: EDiagnosis): Diagnosis_ES = {
    Diagnosis_ES(
      ageAtEventDays = diagnosis.ageAtEventDays,
      diagnosisCategory = diagnosis.diagnosisCategory,
      createdAt = diagnosis.createdAt,
      diagnosis = diagnosis.diagnosis,
      modifiedAt = diagnosis.modifiedAt,
      externalId = diagnosis.externalId,
      kfId = diagnosis.kfId,
      tumorLocation = diagnosis.tumorLocation,
      acdId = diagnosis.acdId,
      mondoId = diagnosis.mondoId,
      uberonId = diagnosis.uberonId
    )
  }
  
  def EOutcomeToOutcomeES(outcome: EOutcome): Outcome_ES = {
    Outcome_ES(
      ageAtEventDays = outcome.ageAtEventDays,
      createdAt = outcome.createdAt,
      diseaseRelated = outcome.diseaseRelated,
      kfId = outcome.kfId,
      modifiedAt = outcome.modifiedAt,
      participantId = outcome.participantId,
      vitalStatus = outcome.vitalStatus
    )
  }
  
  def EGenomicFileToGenomicFileES(gf: EGenomicFile): GenomicFile_ES = {
    GenomicFile_ES(
      controlledAccess = gf.controlledAccess,
      createdAt = gf.createdAt,
      dataType = gf.dataType,
      fileFormat = gf.fileFormat,
      fileName = gf.fileName,
      size = gf.size,
      kfId = gf.kfId,
      modifiedAt = gf.modifiedAt,
//      repeated SequencingExperiment_ES sequencing_experiments = 9;
      referenceGenome = gf.referenceGenome,
      isHarmonized = gf.isHarmonized
    )
  }

  def ESequencingExperimentToSequencingExperimentES(seqExp: ESequencingExperiment): SequencingExperiment_ES = {
    SequencingExperiment_ES(
      kfId = seqExp.kfId,
      experimentDate = seqExp.experimentDate,
      experimentStrategy = seqExp.experimentStrategy,
      sequencingCenterId = seqExp.sequencingCenterId,
      libraryName = seqExp.libraryName,
      libraryStrand = seqExp.libraryStrand,
      isPairedEnd = seqExp.isPairedEnd,
      platform = seqExp.platform,
      instrumentModel = seqExp.instrumentModel,
      maxInsertSize = seqExp.maxInsertSize,
      meanInsertSize = seqExp.meanInsertSize,
      meanDepth = seqExp.meanDepth,
      totalReads = seqExp.totalReads,
      meanReadLength = seqExp.meanReadLength,
      externalId = seqExp.externalId,
      createdAt = seqExp.createdAt,
      modifiedAt = seqExp.modifiedAt
    )
  }

}
