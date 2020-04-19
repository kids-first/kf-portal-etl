package io.kf.etl.models.dataservice

case class EInvestigator(
                          kf_id: scala.Option[String] = None,
                          modified_at: scala.Option[String] = None,
                          created_at: scala.Option[String] = None,
                          institution: scala.Option[String] = None,
                          name: scala.Option[String] = None,
                          studies: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                          visible: scala.Option[Boolean] = None
    )
