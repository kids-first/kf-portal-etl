package io.kf.etl.models.dataservice

case class EOutcome(
                     kf_id: scala.Option[String] = None,
                     modified_at: scala.Option[String] = None,
                     created_at: scala.Option[String] = None,
                     age_at_event_days: scala.Option[Int] = None,
                     disease_related: scala.Option[String] = None,
                     participant_id: scala.Option[String] = None,
                     vital_status: scala.Option[String] = None,
                     visible: scala.Option[Boolean] = None
    )
