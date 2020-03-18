package io.kf.etl.models.dataservice

import io.kf.etl.models.es.ObservableAtAge

case class EPhenotype(
    kfId: Option[String] = None,
    modifiedAt: Option[String] = None,
    createdAt: Option[String] = None,
    ageAtEventDays: Option[Int] = None,
    hpoIdPhenotype: Option[String] = None,
    observed: Option[String] = None,
    participantId: Option[String] = None,
    sourceTextPhenotype: Option[String] = None,
    snomedIdPhenotype: Option[String] = None,
    externalId: Option[String] = None,
    visible: Option[Boolean] = None
    ) extends ObservableAtAge
