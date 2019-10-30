package io.kf.etl.models.es

final case class SequencingExperiment_ES(
                                          kf_id: scala.Option[String] = None,
                                          experiment_date: scala.Option[String] = None,
                                          experiment_strategy: scala.Option[String] = None,
                                          sequencing_center_id: scala.Option[String] = None,
                                          library_name: scala.Option[String] = None,
                                          library_strand: scala.Option[String] = None,
                                          is_paired_end: scala.Option[Boolean] = None,
                                          platform: scala.Option[String] = None,
                                          instrument_model: scala.Option[String] = None,
                                          max_insert_size: scala.Option[Long] = None,
                                          mean_insert_size: scala.Option[Double] = None,
                                          mean_depth: scala.Option[Double] = None,
                                          total_reads: scala.Option[Long] = None,
                                          mean_read_length: scala.Option[Double] = None,
                                          external_id: scala.Option[String] = None
                                        )
