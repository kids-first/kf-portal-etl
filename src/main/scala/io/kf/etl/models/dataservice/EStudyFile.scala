package io.kf.etl.models.dataservice

case class EStudyFile(
                       kf_id: scala.Option[String] = None,
                       modified_at: scala.Option[String] = None,
                       created_at: scala.Option[String] = None,
                       file_name: scala.Option[String] = None,
                       study_id: scala.Option[String] = None,
                       visible: scala.Option[Boolean] = None
    )
