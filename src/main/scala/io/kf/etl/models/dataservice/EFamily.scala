package io.kf.etl.models.dataservice

case class EFamily(
                    kf_id: scala.Option[String] = None,
                    modified_at: scala.Option[String] = None,
                    created_at: scala.Option[String] = None,
                    participants: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                    visible: scala.Option[Boolean] = None
    )
