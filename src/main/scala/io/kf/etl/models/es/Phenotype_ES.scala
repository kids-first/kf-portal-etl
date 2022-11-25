package io.kf.etl.models.es

final case class Phenotype_ES(
                               age_at_event_days: Option[Int] = None,
                               ancestral_hpo_ids: Option[String] = None,
                               external_id: Option[String] = None,
                               hpo_phenotype_not_observed: Option[String] = None,
                               hpo_phenotype_observed: Option[String] = None,
                               hpo_phenotype_observed_text: Option[String] = None,
                               shared_hpo_ids: Option[String] = None,
                               snomed_phenotype_not_observed: Option[String] = None,
                               snomed_phenotype_observed: Option[String] = None,
                               source_text_phenotype: Option[String] = None,
                               source_text_not_observed_phenotype: Option[String] = None,
                               observed: Option[Boolean] = None
                             )
