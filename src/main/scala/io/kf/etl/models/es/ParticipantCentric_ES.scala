package io.kf.etl.models.es

final case class ParticipantCentric_ES(
    affected_status: Option[Boolean] = None,
    alias_group: Option[String] = None,
    available_data_types: Seq[String] = Nil,
    biospecimens: Seq[BiospecimenCombined_ES] = Nil,
    diagnoses: _root_.scala.collection.Seq[Diagnosis_ES] = _root_.scala.collection.Seq.empty,
    diagnosis_category: scala.Option[String] = None,
    ethnicity: scala.Option[String] = None,
    external_id: scala.Option[String] = None,
    family_id: scala.Option[String] = None,
    family: scala.Option[Family_ES] = None,
    files: Seq[GenomicFile_ES] = Nil,
    gender: scala.Option[String] = None,
    is_proband: scala.Option[Boolean] = None,
    kf_id: scala.Option[String] = None,
    outcome: scala.Option[Outcome_ES] = None,
    phenotype: _root_.scala.collection.Seq[Phenotype_ES] = _root_.scala.collection.Seq.empty,
    race: scala.Option[String] = None,
    study: scala.Option[Study_ES] = None
    )
