package io.kf.etl.models.dataservice

case class EDiagnosis(
    ageAtEventDays: scala.Option[Int] = None,
    createdAt: scala.Option[String] = None,
    diagnosisCategory: scala.Option[String] = None,
    externalId: scala.Option[String] = None,
    icdIdDiagnosis: scala.Option[String] = None,
    kfId: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    mondoIdDiagnosis: scala.Option[String] = None,
    participantId: scala.Option[String] = None,
    sourceTextDiagnosis: scala.Option[String] = None,
    uberonIdTumorLocation: scala.Option[String] = None,
    sourceTextTumorLocation: scala.Option[String] = None,
    ncitIdDiagnosis: scala.Option[String] = None,
    spatialDescriptor: scala.Option[String] = None,
    diagnosisText: scala.Option[String] = None,
    biospecimens: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    visible: scala.Option[Boolean] = None
    )
