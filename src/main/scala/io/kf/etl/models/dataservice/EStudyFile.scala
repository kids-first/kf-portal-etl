package io.kf.etl.models.dataservice

case class EStudyFile(
    kfId: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    createdAt: scala.Option[String] = None,
    fileName: scala.Option[String] = None,
    studyId: scala.Option[String] = None,
    visible: scala.Option[Boolean] = None
    )
