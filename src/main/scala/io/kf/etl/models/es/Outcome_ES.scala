package io.kf.etl.models.es

final case class Outcome_ES(
                             age_at_event_days: scala.Option[Int] = None,
                             disease_related: scala.Option[String] = None,
                             kf_id: scala.Option[String] = None,
                             vital_status: scala.Option[String] = None
                           )
