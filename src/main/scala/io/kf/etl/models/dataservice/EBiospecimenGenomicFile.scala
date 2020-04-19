package io.kf.etl.models.dataservice

case class EBiospecimenGenomicFile(
                                    kf_id: scala.Option[String] = None,
                                    biospecimen_id: scala.Option[String] = None,
                                    genomic_file_id: scala.Option[String] = None,
                                    visible: scala.Option[Boolean] = None
    )
