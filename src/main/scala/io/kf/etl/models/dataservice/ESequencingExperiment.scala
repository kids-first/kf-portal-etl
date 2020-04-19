package io.kf.etl.models.dataservice

case class ESequencingExperiment(
                                  kf_id: Option[String] = None,
                                  created_at: Option[String] = None,
                                  modified_at: Option[String] = None,
                                  experiment_date: Option[String] = None,
                                  experiment_strategy: Option[String] = None,
                                  center: Option[String] = None,
                                  library_name: Option[String] = None,
                                  library_prep: Option[String] = None,
                                  library_selection: Option[String] = None,
                                  library_strand: Option[String] = None,
                                  is_paired_end: Option[Boolean] = None,
                                  platform: Option[String] = None,
                                  instrument_model: Option[String] = None,
                                  max_insert_size: Option[Long] = None,
                                  mean_insert_size: Option[Double] = None,
                                  mean_depth: Option[Double] = None,
                                  total_reads: Option[Long] = None,
                                  mean_read_length: Option[Double] = None,
                                  external_id: Option[String] = None,
                                  genomic_files: Seq[String] = Nil,
                                  sequencing_center_id: Option[String] = None,
                                  visible: Option[Boolean] = None
    )
