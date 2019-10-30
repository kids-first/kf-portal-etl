package io.kf.etl.models.dataservice

case class ESequencingExperiment(
    kfId: scala.Option[String] = None,
    createdAt: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    experimentDate: scala.Option[String] = None,
    experimentStrategy: scala.Option[String] = None,
    center: scala.Option[String] = None,
    libraryName: scala.Option[String] = None,
    libraryStrand: scala.Option[String] = None,
    isPairedEnd: scala.Option[Boolean] = None,
    platform: scala.Option[String] = None,
    instrumentModel: scala.Option[String] = None,
    maxInsertSize: scala.Option[Long] = None,
    meanInsertSize: scala.Option[Double] = None,
    meanDepth: scala.Option[Double] = None,
    totalReads: scala.Option[Long] = None,
    meanReadLength: scala.Option[Double] = None,
    externalId: scala.Option[String] = None,
    genomicFiles: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    sequencingCenterId: scala.Option[String] = None,
    visible: scala.Option[Boolean] = None
    )
