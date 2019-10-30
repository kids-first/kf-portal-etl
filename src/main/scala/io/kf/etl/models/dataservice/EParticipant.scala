package io.kf.etl.models.dataservice

case class EParticipant(
    affectedStatus: scala.Option[Boolean] = None,
    aliasGroup: scala.Option[String] = None,
    biospecimens: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    createdAt: scala.Option[String] = None,
    diagnoses: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    diagnosisCategory: scala.Option[String] = None,
    ethnicity: scala.Option[String] = None,
    externalId: scala.Option[String] = None,
    familyId: scala.Option[String] = None,
    gender: scala.Option[String] = None,
    isProband: scala.Option[Boolean] = None,
    kfId: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    outcomes: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    phenotypes: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
    race: scala.Option[String] = None,
    studyId: scala.Option[String] = None,
    visible: scala.Option[Boolean] = None
    )
