package io.kf.etl.models.dataservice

case class EGenomicFile(
    acl: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    accessUrls: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    availability: scala.Option[String] = None,
    controlledAccess: scala.Option[Boolean] = None,
    createdAt: scala.Option[String] = None,
    dataType: scala.Option[String] = None,
    externalId: scala.Option[String] = None,
    fileFormat: scala.Option[String] = None,
    fileName: scala.Option[String] = None,
    hashes: scala.Option[EHash] = None,
    instrumentModels: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    isHarmonized: scala.Option[Boolean] = None,
    isPairedEnd: scala.Option[Boolean] = None,
    kfId: scala.Option[String] = None,
    latestDid: scala.Option[String] = None,
    metadata: scala.Option[EMetadata] = None,
    modifiedAt: scala.Option[String] = None,
    platforms: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    referenceGenome: scala.Option[String] = None,
    repository: scala.Option[String] = None,
    size: scala.Option[Long] = None,
    urls: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    visible: scala.Option[Boolean] = None
    )
