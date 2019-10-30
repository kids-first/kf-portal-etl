package io.kf.etl.models.dataservice

case class EStudy(
    attribution: scala.Option[String] = None,
    createdAt: scala.Option[String] = None,
    dataAccessAuthority: scala.Option[String] = None,
    externalId: scala.Option[String] = None,
    kfId: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    name: scala.Option[String] = None,
    participants: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    releaseStatus: scala.Option[String] = None,
    studyFiles: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    version: scala.Option[String] = None,
    shortName: scala.Option[String] = None,
    visible: scala.Option[Boolean] = None
    )
