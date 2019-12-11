package io.kf.etl.processors.common.converter

import io.kf.etl.models.dataservice._
import io.kf.etl.models.es._

object EntityConverter {
  
  def EStudyToStudyES(study: EStudy): Study_ES = {
    Study_ES(
      kf_id = study.kfId,
      attribution = study.attribution,
      name = study.name,
      version = study.version,
      external_id = study.externalId,
      release_status = study.releaseStatus,
      data_access_authority = study.dataAccessAuthority,
      short_name = study.shortName
    )
  }
  
  def EParticipantToParticipantES(participant: EParticipant): Participant_ES = {
    Participant_ES(
      affected_status = participant.affectedStatus,
      alias_group = participant.aliasGroup,
      diagnosis_category = participant.diagnosisCategory,
      ethnicity = participant.ethnicity,
      external_id = participant.externalId,
      family_id = participant.familyId,
      gender = participant.gender,
      is_proband = participant.isProband,
      kf_id = participant.kfId,
      race = participant.race
    )
  }
  
  def EBiospecimenToBiospecimenES(bio: EBiospecimen): Biospecimen_ES = {
    Biospecimen_ES(
      age_at_event_days = bio.ageAtEventDays,
      analyte_type = bio.analyteType,
      composition = bio.composition,
      concentration_mg_per_ml = bio.concentrationMgPerMl,
      consent_type = bio.consentType,
      dbgap_consent_code = bio.dbgapConsentCode,
      external_aliquot_id = bio.externalAliquotId,
      external_sample_id = bio.externalSampleId,
      kf_id = bio.kfId,
      ncit_id_anatomical_site = bio.ncitIdAnatomicalSite,
      ncit_id_tissue_type = bio.ncitIdTissueType,
      shipment_date = bio.shipmentDate,
      shipment_origin = bio.shipmentOrigin,
      genomic_files = bio.genomicFiles,
      source_text_tumor_descriptor = bio.sourceTextTumorDescriptor,
      source_text_tissue_type = bio.sourceTextTissueType,
      source_text_anatomical_site = bio.sourceTextAnatomicalSite,
      spatial_descriptor = bio.spatialDescriptor,
      uberon_id_anatomical_site = bio.uberonIdAnatomicalSite,
      volume_ml = bio.volumeMl,
      diagnoses = bio.diagnoses.map(EDiagnosisToDiagnosisES),
      sequencing_center_id = bio.sequencingCenterId
    )
  }

  def EBiospecimenToBiospecimenCombinedES(bio: EBiospecimen): BiospecimenCombined_ES = {
    BiospecimenCombined_ES(
      age_at_event_days = bio.ageAtEventDays,
      analyte_type = bio.analyteType,
      composition = bio.composition,
      concentration_mg_per_ml = bio.concentrationMgPerMl,
      consent_type = bio.consentType,
      dbgap_consent_code = bio.dbgapConsentCode,
      external_aliquot_id = bio.externalAliquotId,
      external_sample_id = bio.externalSampleId,
      kf_id = bio.kfId,
      method_of_sample_procurement = bio.methodOfSampleProcurement,
      ncit_id_anatomical_site = bio.ncitIdAnatomicalSite,
      ncit_id_tissue_type = bio.ncitIdTissueType,
      shipment_date = bio.shipmentDate,
      shipment_origin = bio.shipmentOrigin,
      genomic_files = Nil, // gFiles.map(f => EGenomicFileToGenomicFileES(f)),
      source_text_tumor_descriptor = bio.sourceTextTumorDescriptor,
      source_text_tissue_type = bio.sourceTextTissueType,
      source_text_anatomical_site = bio.sourceTextAnatomicalSite,
      spatial_descriptor = bio.spatialDescriptor,
      uberon_id_anatomical_site = bio.uberonIdAnatomicalSite,
      volume_ml = bio.volumeMl,
      diagnoses = bio.diagnoses.map(EDiagnosisToDiagnosisES),
      sequencing_center_id = bio.sequencingCenterId
    )
  }

  def EDiagnosisToDiagnosisES(diagnosis: EDiagnosis): Diagnosis_ES = {
    Diagnosis_ES(

      age_at_event_days = diagnosis.ageAtEventDays,
      diagnosis_category = diagnosis.diagnosisCategory,
      external_id = diagnosis.externalId,
      icd_id_diagnosis = diagnosis.icdIdDiagnosis,
      kf_id = diagnosis.kfId,
      mondo_id_diagnosis = diagnosis.mondoIdDiagnosis,
      source_text_diagnosis = diagnosis.sourceTextDiagnosis,
      uberon_id_tumor_location = diagnosis.uberonIdTumorLocation,
      source_text_tumor_location = diagnosis.sourceTextTumorLocation,
      ncit_id_diagnosis = diagnosis.ncitIdDiagnosis,
      spatial_descriptor = diagnosis.spatialDescriptor,
      diagnosis = diagnosis.diagnosisText,
      biospecimens = diagnosis.biospecimens
    )
  }
  
  def EOutcomeToOutcomeES(outcome: EOutcome): Outcome_ES = {
    Outcome_ES(
      age_at_event_days = outcome.ageAtEventDays,
      disease_related = outcome.diseaseRelated,
      kf_id = outcome.kfId,
      vital_status = outcome.vitalStatus
    )
  }

  def EGenomicFileToGenomicFileES(gf: EGenomicFile): GenomicFile_ES = {
    EGenomicFileToGenomicFileES(gf, Seq(SequencingExperiment_ES()))
  }

  def EGenomicFileToGenomicFileES(gf: EGenomicFile, seqExps: Seq[SequencingExperiment_ES]): GenomicFile_ES = {
    GenomicFile_ES(
      acl = gf.acl,
      access_urls = gf.accessUrls,
      availability = gf.availability,
      controlled_access = gf.controlledAccess,
      data_type = gf.dataType,
      external_id = gf.externalId,
      file_format = gf.fileFormat,
      file_name = gf.fileName,
      instrument_models = gf.instrumentModels,
      is_harmonized = gf.isHarmonized,
      is_paired_end = gf.isPairedEnd,
      kf_id = gf.kfId,
      latest_did = gf.latestDid,
      platforms = gf.platforms,
      reference_genome = gf.referenceGenome,
      repository = gf.repository,
      sequencing_experiments = seqExps,
      size = gf.size
    )
  }

  def ESequencingExperimentToSequencingExperimentES(seqExp: ESequencingExperiment): SequencingExperiment_ES = {
    SequencingExperiment_ES(
      kf_id = seqExp.kfId,
      experiment_date = seqExp.experimentDate,
      experiment_strategy = seqExp.experimentStrategy,
      sequencing_center_id = seqExp.sequencingCenterId,
      library_name = seqExp.libraryName,
      library_strand = seqExp.libraryStrand,
      is_paired_end = seqExp.isPairedEnd,
      platform = seqExp.platform,
      instrument_model = seqExp.instrumentModel,
      max_insert_size = seqExp.maxInsertSize,
      mean_insert_size = seqExp.meanInsertSize,
      mean_depth = seqExp.meanDepth,
      total_reads = seqExp.totalReads,
      mean_read_length = seqExp.meanReadLength,
      external_id = seqExp.externalId
    )
  }
  
  def EGenomicFileToFileCentricES(genomicFile: EGenomicFile, seqExps: Seq[SequencingExperiment_ES], participants: Seq[Participant_ES]): FileCentric_ES = {
    FileCentric_ES(
      acl = genomicFile.acl,
      access_urls = genomicFile.accessUrls,
      availability = genomicFile.availability,
      controlled_access = genomicFile.controlledAccess,
      data_type = genomicFile.dataType,
      external_id = genomicFile.externalId,
      file_format = genomicFile.fileFormat,
      file_name = genomicFile.fileName,
      instrument_models = genomicFile.instrumentModels,
      is_harmonized = genomicFile.isHarmonized,
      is_paired_end = genomicFile.isPairedEnd,
      kf_id = genomicFile.kfId,
      latest_did = genomicFile.latestDid,
      participants = participants,
      platforms = genomicFile.platforms,
      reference_genome = genomicFile.referenceGenome,
      repository = genomicFile.repository,
      sequencing_experiments = seqExps,
      size = genomicFile.size
    )
  }

}
