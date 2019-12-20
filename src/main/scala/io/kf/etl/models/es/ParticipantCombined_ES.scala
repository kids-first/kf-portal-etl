package io.kf.etl.models.es

final case class ParticipantCombined_ES (
                                          affected_status: Option[Boolean] = None,
                                          alias_group: Option[String] = None,
                                          available_data_types: Seq[String] = Nil,
                                          biospecimens: Seq[BiospecimenCombined_ES] = Nil,
                                          diagnoses: Seq[Diagnosis_ES] = Nil,
                                          diagnosis_category: Option[String] = None,
                                          ethnicity: Option[String] = None,
                                          external_id: Option[String] = None,
                                          family: Option[Family_ES] = None,
                                          family_id: Option[String] = None,
                                          files: Seq[GenomicFile_ES] = Nil,
                                          gender: Option[String] = None,
                                          is_proband: Option[Boolean] = None,
                                          kf_id: String,
                                          outcome: Option[Outcome_ES] = None,
                                          phenotype: Seq[Phenotype_ES] = Nil,
                                          race: Option[String] = None,
                                          study: Option[Study_ES] = None
                                        )