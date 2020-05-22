package io.kf.etl.models.dataservice

case class EPhenotype(
                       kf_id: Option[String] = None,
                       modified_at: Option[String] = None,
                       created_at: Option[String] = None,
                       age_at_event_days: Option[Int] = None,
                       hpo_id_phenotype: Option[String] = None,
                       observed: Option[String] = None,
                       participant_id: Option[String] = None,
                       source_text_phenotype: Option[String] = None,
                       snomed_id_phenotype: Option[String] = None,
                       external_id: Option[String] = None,
                       visible: Option[Boolean] = None
    ) extends ObservableAtAge
