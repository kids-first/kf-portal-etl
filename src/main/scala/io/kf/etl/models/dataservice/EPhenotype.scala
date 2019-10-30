package io.kf.etl.models.dataservice

case class EPhenotype(
    kfId: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    createdAt: scala.Option[String] = None,
    ageAtEventDays: scala.Option[Int] = None,
    hpoIdPhenotype: scala.Option[String] = None,
    observed: scala.Option[String] = None,
    participantId: scala.Option[String] = None,
    sourceTextPhenotype: scala.Option[String] = None,
    snomedIdPhenotype: scala.Option[String] = None,
    externalId: scala.Option[String] = None,
    visible: scala.Option[Boolean] = None
    )
