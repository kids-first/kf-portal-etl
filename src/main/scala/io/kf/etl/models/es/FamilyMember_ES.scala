package io.kf.etl.models.es

final case class FamilyMember_ES(
                                  alias_group: scala.Option[String] = None,
                                  relationship: scala.Option[String] = None,
                                  consent_type: scala.Option[String] = None,
                                  diagnoses: _root_.scala.collection.Seq[Diagnosis_ES] = _root_.scala.collection.Seq.empty,
                                  ethnicity: scala.Option[String] = None,
                                  external_id: scala.Option[String] = None,
                                  gender: scala.Option[String] = None,
                                  is_proband: scala.Option[Boolean] = None,
                                  kf_id: scala.Option[String] = None,
                                  outcome: scala.Option[Outcome_ES] = None,
                                  phenotype: _root_.scala.collection.Seq[Phenotype_ES] = _root_.scala.collection.Seq.empty,
                                  race: scala.Option[String] = None,
                                  available_data_types: _root_.scala.collection.Seq[String] = Nil)
