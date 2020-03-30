package io.kf.etl.models.dataservice

import io.kf.etl.models.es.ObservableAtAge

case class EDiagnosis(
    ageAtEventDays: Option[Int] = None,
    createdAt: Option[String] = None,
    diagnosisCategory: Option[String] = None,
    externalId: Option[String] = None,
    icdIdDiagnosis: Option[String] = None,
    kfId: Option[String] = None,
    modifiedAt: Option[String] = None,
    mondoIdDiagnosis: Option[String] = None,
    participantId: Option[String] = None,
    sourceTextDiagnosis: Option[String] = None,
    uberonIdTumorLocation: Option[String] = None,
    sourceTextTumorLocation: Option[String] = None,
    ncitIdDiagnosis: Option[String] = None,
    spatialDescriptor: Option[String] = None,
    diagnosisText: Option[String] = None,
    biospecimens: _root_.scala.collection.Seq[String] = Nil,
    visible: Option[Boolean] = None
    ) extends ObservableAtAge
