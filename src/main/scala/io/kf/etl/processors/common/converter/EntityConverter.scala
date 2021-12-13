package io.kf.etl.processors.common.converter

import io.kf.etl.models.dataservice._
import io.kf.etl.models.es._

object EntityConverter {

  val domainMappingTable: Map[String, String] = Map(
    "UNKNOWN" -> "Unknown",
    "CANCER" -> "Cancer",
    "BIRTHDEFECT" -> "Birth Defect",
    "COVID19" -> "COVID-19",
    "OTHER" -> "Other"
  )

  def EStudyToStudyES(study: EStudy): Study_ES = {
    Study_ES(
      kf_id = study.kf_id,
      attribution = study.attribution,
      name = study.program.map(program => s"$program: ${study.name}").orElse(study.name),
      version = study.version,
      external_id = study.external_id,
      release_status = study.release_status,
      data_access_authority = study.data_access_authority,
      short_name = study.short_name,
      code = study.short_code,
      domain = study.domain.map(_.split("AND").map(domain => domainMappingTable(domain)).toSeq).getOrElse(Seq.empty),
      program = study.program
    )
  }

  def EParticipantToParticipantES(participant: EParticipant): Participant_ES = {
    Participant_ES(
      affected_status = participant.affected_status,
      alias_group = participant.alias_group,
      diagnosis_category = participant.diagnosis_category,
      ethnicity = participant.ethnicity,
      external_id = participant.external_id,
      family_id = participant.family_id,
      gender = participant.gender,
      is_proband = participant.is_proband,
      kf_id = participant.kf_id,
      race = participant.race
    )
  }

  def EBiospecimenToBiospecimenES(
      bio: EBiospecimen,
      gfiles: Seq[GenomicFile_ES] = Nil
  ): Biospecimen_ES = {
    Biospecimen_ES(
      age_at_event_days = bio.age_at_event_days,
      analyte_type = bio.analyte_type,
      composition = bio.composition,
      concentration_mg_per_ml = bio.concentration_mg_per_ml,
      consent_type = bio.consent_type,
      duo_code = bio.duo_ids,
      dbgap_consent_code = bio.dbgap_consent_code,
      external_aliquot_id = bio.external_aliquot_id,
      external_sample_id = bio.external_sample_id,
      kf_id = bio.kf_id,
      method_of_sample_procurement = bio.method_of_sample_procurement,
      ncit_id_anatomical_site = bio.ncit_id_anatomical_site,
      ncit_id_tissue_type = bio.ncit_id_tissue_type,
      shipment_date = bio.shipment_date,
      shipment_origin = bio.shipment_origin,
      genomic_files = gfiles,
      source_text_tumor_descriptor = bio.source_text_tumor_descriptor,
      source_text_tissue_type = bio.source_text_tissue_type,
      source_text_anatomical_site = bio.source_text_anatomical_site.map(_.split(";").toSeq).getOrElse(Seq.empty),
      spatial_descriptor = bio.spatial_descriptor,
      uberon_id_anatomical_site = bio.uberon_id_anatomical_site,
      volume_ul = bio.volume_ul,
      diagnoses = bio.diagnoses.map(d => EDiagnosisToDiagnosisES(d, None)),
      sequencing_center_id = bio.sequencing_center_id,
      sequencing_center = bio.sequencing_center
    )
  }

  def EDiagnosisToDiagnosisES(
      diagnosis: EDiagnosis,
      diagnosisTermWithParents_ES: Option[DiagnosisTermWithParents_ES]
  ): Diagnosis_ES = {
    val mondoIdDiagnosos =
      (diagnosis.mondo_id_diagnosis, diagnosis.diagnosis_text) match {
        case (Some(id), Some(s)) => Some(s"$s ($id)")
        case (Some(id), None)    => Some(s"$id")
        case _                   => None
      }
    Diagnosis_ES(
      age_at_event_days = diagnosis.age_at_event_days,
      diagnosis_category = diagnosis.diagnosis_category,
      external_id = diagnosis.external_id,
      icd_id_diagnosis = diagnosis.icd_id_diagnosis,
      kf_id = diagnosis.kf_id,
      mondo_id_diagnosis = mondoIdDiagnosos,
      source_text_diagnosis = diagnosis.source_text_diagnosis,
      uberon_id_tumor_location = diagnosis.uberon_id_tumor_location,
      source_text_tumor_location = diagnosis.source_text_tumor_location.map(_.split(";").toSeq).getOrElse(Seq.empty),
      ncit_id_diagnosis = diagnosis.ncit_id_diagnosis,
      spatial_descriptor = diagnosis.spatial_descriptor,
      diagnosis = diagnosis.diagnosis_text,
      biospecimens = diagnosis.biospecimens,
      is_tagged = diagnosisTermWithParents_ES match {
        case Some(d) => d.is_tagged
        case None    => false
      },
      mondo = diagnosisTermWithParents_ES match {
        case Some(d) => Seq(d)
        case None    => Seq.empty[DiagnosisTermWithParents_ES]
      }
    )
  }

  def EDiagnosisToLightDiagnosisES(
      diagnosis: EDiagnosis,
      diagnosisTermWithParents_ES: Option[DiagnosisTermWithParents_ES]
  ): Diagnosis_ES = {
    val mondoIdDiagnosos =
      (diagnosis.mondo_id_diagnosis, diagnosis.diagnosis_text) match {
        case (Some(id), Some(s)) => Some(s"$s ($id)")
        case (Some(id), None)    => Some(s"$id")
        case _                   => None
      }
    Diagnosis_ES(
      diagnosis_category = diagnosis.diagnosis_category,
      age_at_event_days = diagnosis.age_at_event_days,
      source_text_tumor_location = diagnosis.source_text_tumor_location.map(_.split(";").toSeq).getOrElse(Seq.empty),
      ncit_id_diagnosis = diagnosis.ncit_id_diagnosis,
      source_text_diagnosis = diagnosis.source_text_diagnosis,
      mondo_id_diagnosis = mondoIdDiagnosos,
      is_tagged = diagnosisTermWithParents_ES match {
        case Some(d) => d.is_tagged
        case None    => false
      },
      mondo = diagnosisTermWithParents_ES match {
        case Some(d) => Seq(d)
        case None    => Seq.empty[DiagnosisTermWithParents_ES]
      }
    )
  }

  def EOutcomeToOutcomeES(outcome: EOutcome): Outcome_ES = {
    Outcome_ES(
      age_at_event_days = outcome.age_at_event_days,
      disease_related = outcome.disease_related,
      kf_id = outcome.kf_id,
      vital_status = outcome.vital_status
    )
  }

  def EGenomicFileToGenomicFileES(gf: EGenomicFile): GenomicFile_ES = {
    EGenomicFileToGenomicFileES(gf, Seq(SequencingExperiment_ES()))
  }

  def EGenomicFileToGenomicFileES(
      gf: EGenomicFile,
      seqExps: Seq[SequencingExperiment_ES]
  ): GenomicFile_ES = {
    GenomicFile_ES(
      acl = gf.acl,
      access_urls = gf.access_urls,
      availability = gf.availability,
      controlled_access = gf.controlled_access,
      data_type = gf.data_type,
      external_id = gf.external_id,
      file_format = gf.file_format,
      file_name = gf.file_name,
      instrument_models = gf.instrument_models,
      is_harmonized = gf.is_harmonized,
      is_paired_end = gf.is_paired_end,
      kf_id = gf.kf_id,
      latest_did = gf.latest_did,
      platforms = gf.platforms,
      reference_genome = gf.reference_genome,
      repository = gf.repository,
      sequencing_experiments = seqExps,
      size = gf.size
    )
  }

  def ESequencingExperimentToSequencingExperimentES(
      seqExp: ESequencingExperiment
  ): SequencingExperiment_ES = {
    SequencingExperiment_ES(
      kf_id = seqExp.kf_id,
      experiment_date = seqExp.experiment_date,
      experiment_strategy = seqExp.experiment_strategy,
      sequencing_center_id = seqExp.sequencing_center_id,
      library_name = seqExp.library_name,
      library_prep = seqExp.library_prep,
      library_selection = seqExp.library_selection,
      library_strand = seqExp.library_strand,
      is_paired_end = seqExp.is_paired_end,
      platform = seqExp.platform,
      instrument_model = seqExp.instrument_model,
      max_insert_size = seqExp.max_insert_size,
      mean_insert_size = seqExp.mean_insert_size,
      mean_depth = seqExp.mean_depth,
      total_reads = seqExp.total_reads,
      mean_read_length = seqExp.mean_read_length,
      external_id = seqExp.external_id
    )
  }

  def EGenomicFileToFileCentricES(
      genomicFile: EGenomicFile,
      seqExps: Seq[SequencingExperiment_ES],
      participants: Seq[Participant_ES]
  ): FileCentric_ES = {
    FileCentric_ES(
      acl = genomicFile.acl,
      access_urls = genomicFile.access_urls,
      availability = genomicFile.availability,
      controlled_access = genomicFile.controlled_access,
      data_type = genomicFile.data_type,
      external_id = genomicFile.external_id,
      file_format = genomicFile.file_format,
      file_name = genomicFile.file_name,
      instrument_models = genomicFile.instrument_models,
      is_harmonized = genomicFile.is_harmonized,
      is_paired_end = genomicFile.is_paired_end,
      kf_id = genomicFile.kf_id,
      latest_did = genomicFile.latest_did,
      participants = participants,
      platforms = genomicFile.platforms,
      reference_genome = genomicFile.reference_genome,
      repository = genomicFile.repository,
      sequencing_experiments = seqExps,
      size = genomicFile.size
    )
  }

}
