package io.kf.etl.models.dataservice

case class ESequencingExperimentGenomicFile(
    createdAt: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    visible: scala.Option[Boolean] = None,
    externalId: scala.Option[String] = None,
    genomicFile: scala.Option[String] = None,
    kfId: scala.Option[String] = None,
    sequencingExperiment: scala.Option[String] = None
    )
