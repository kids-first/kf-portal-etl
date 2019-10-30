package io.kf.etl.models.dataservice

case class EOutcome(
    kfId: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    createdAt: scala.Option[String] = None,
    ageAtEventDays: scala.Option[Int] = None,
    diseaseRelated: scala.Option[String] = None,
    participantId: scala.Option[String] = None,
    vitalStatus: scala.Option[String] = None,
    visible: scala.Option[Boolean] = None
    )
