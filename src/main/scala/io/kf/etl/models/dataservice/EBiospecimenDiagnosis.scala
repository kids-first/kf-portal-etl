package io.kf.etl.models.dataservice

case class EBiospecimenDiagnosis(
                                  kf_id: scala.Option[String] = None,
                                  biospecimen_id: scala.Option[String] = None,
                                  diagnosis_id: scala.Option[String] = None,
                                  visible: scala.Option[Boolean] = None
    )
