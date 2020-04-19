package io.kf.etl.models.dataservice

case class EStudy(
                   attribution: scala.Option[String] = None,
                   created_at: scala.Option[String] = None,
                   data_access_authority: scala.Option[String] = None,
                   external_id: scala.Option[String] = None,
                   kf_id: scala.Option[String] = None,
                   modified_at: scala.Option[String] = None,
                   name: scala.Option[String] = None,
                   participants: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                   release_status: scala.Option[String] = None,
                   study_files: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                   version: scala.Option[String] = None,
                   short_name: scala.Option[String] = None,
                   visible: scala.Option[Boolean] = None
    )
