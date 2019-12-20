package io.kf.etl.models.es

final case class SequencingExperiment_ES(
                                          kf_id: Option[String] = None,
                                          experiment_date: Option[String] = None,
                                          experiment_strategy: Option[String] = None,
                                          sequencing_center_id: Option[String] = None,
                                          library_name: Option[String] = None,
                                          library_strand: Option[String] = None,
                                          is_paired_end: Option[Boolean] = None,
                                          platform: Option[String] = None,
                                          instrument_model: Option[String] = None,
                                          max_insert_size: Option[Long] = None,
                                          mean_insert_size: Option[Double] = None,
                                          mean_depth: Option[Double] = None,
                                          total_reads: Option[Long] = None,
                                          mean_read_length: Option[Double] = None,
                                          external_id: Option[String] = None
                                        )
