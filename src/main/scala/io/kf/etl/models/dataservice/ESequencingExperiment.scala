package io.kf.etl.models.dataservice

case class ESequencingExperiment(
    kfId: Option[String] = None,
    createdAt: Option[String] = None,
    modifiedAt: Option[String] = None,
    experimentDate: Option[String] = None,
    experimentStrategy: Option[String] = None,
    center: Option[String] = None,
    libraryName: Option[String] = None,
    library_prep: Option[String] = None,
    library_selection: Option[String] = None,
    libraryStrand: Option[String] = None,
    isPairedEnd: Option[Boolean] = None,
    platform: Option[String] = None,
    instrumentModel: Option[String] = None,
    maxInsertSize: Option[Long] = None,
    meanInsertSize: Option[Double] = None,
    meanDepth: Option[Double] = None,
    totalReads: Option[Long] = None,
    meanReadLength: Option[Double] = None,
    externalId: Option[String] = None,
    genomicFiles: Seq[String] = Nil,
    sequencingCenterId: Option[String] = None,
    visible: Option[Boolean] = None
    )
