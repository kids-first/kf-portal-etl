package io.kf.etl.models.dataservice

case class EInvestigator(
    kfId: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    createdAt: scala.Option[String] = None,
    institution: scala.Option[String] = None,
    name: scala.Option[String] = None,
    studies: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    visible: scala.Option[Boolean] = None
    )
