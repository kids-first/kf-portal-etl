package io.kf.etl.models.es

final case class Participant_ES(
                                 affected_status: scala.Option[Boolean] = None,
                                 alias_group: scala.Option[String] = None,
                                 available_data_types: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                 biospecimens: Seq[Biospecimen_ES] = Nil,
                                 diagnoses: _root_.scala.collection.Seq[Diagnosis_ES] = _root_.scala.collection.Seq.empty,
                                 diagnosis_category: scala.Option[String] = None,
                                 ethnicity: scala.Option[String] = None,
                                 external_id: scala.Option[String] = None,
                                 family: scala.Option[Family_ES] = None,
                                 family_id: scala.Option[String] = None,
                                 gender: scala.Option[String] = None,
                                 is_proband: scala.Option[Boolean] = None,
                                 kf_id: scala.Option[String] = None,
                                 outcome: scala.Option[Outcome_ES] = None,
                                 phenotype: _root_.scala.collection.Seq[Phenotype_ES] = _root_.scala.collection.Seq.empty,
                                 race: scala.Option[String] = None,
                                 study: scala.Option[Study_ES] = None
                               )
