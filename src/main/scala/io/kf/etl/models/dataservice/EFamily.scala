package io.kf.etl.models.dataservice

case class EFamily(
    kfId: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    createdAt: scala.Option[String] = None,
    participants: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    visible: scala.Option[Boolean] = None
    )
