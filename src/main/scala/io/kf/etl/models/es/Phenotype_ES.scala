package io.kf.etl.models.es

final case class Phenotype_ES(
                               age_at_event_days: scala.Option[Int] = None,
                               ancestral_hpo_ids: scala.Option[String] = None,
                               external_id: scala.Option[String] = None,
                               hpo_phenotype_not_observed: scala.Option[String] = None,
                               hpo_phenotype_observed: scala.Option[String] = None,
                               hpo_phenotype_observed_text: scala.Option[String] = None,
                               shared_hpo_ids: scala.Option[String] = None,
                               snomed_phenotype_not_observed: scala.Option[String] = None,
                               snomed_phenotype_observed: scala.Option[String] = None,
                               source_text_phenotype: scala.Option[String] = None,
                               observed: scala.Option[Boolean] = None
                             )
