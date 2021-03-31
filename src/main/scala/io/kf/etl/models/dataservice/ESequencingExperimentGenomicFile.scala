package io.kf.etl.models.dataservice

case class ESequencingExperimentGenomicFile(created_at: Option[String] = None,
                                            modified_at: Option[String] = None,
                                            visible: Option[Boolean] = None,
                                            external_id: Option[String] = None,
                                            genomic_file: Option[String] = None,
                                            kf_id: Option[String] = None,
                                            sequencing_experiment: Option[String] = None)
