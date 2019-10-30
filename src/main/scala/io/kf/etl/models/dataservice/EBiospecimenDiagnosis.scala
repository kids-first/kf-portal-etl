package io.kf.etl.models.dataservice

case class EBiospecimenDiagnosis(
    kfId: scala.Option[String] = None,
    biospecimenId: scala.Option[String] = None,
    diagnosisId: scala.Option[String] = None,
    visible: scala.Option[Boolean] = None
    )
