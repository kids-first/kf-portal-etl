package io.kf.etl.models.es

final case class FileCentric_ES(
                                 acl: Seq[String] = Nil,
                                 access_urls: Seq[String] = Nil,
                                 availability: Option[String] = None,
                                 controlled_access: Option[Boolean] = None,
                                 data_type: Option[String] = None,
                                 experiment_strategies: Seq[String] = Nil,
                                 external_id: Option[String] = None,
                                 file_format: Option[String] = None,
                                 file_name: Option[String] = None,
                                 instrument_models: Seq[String] = Nil,
                                 is_paired_end: scala.Option[Boolean] = None,
                                 size: scala.Option[Long] = None,
                                 kf_id: scala.Option[String] = None,
                                 participants: Seq[Participant_ES] = Nil,
                                 platforms: Seq[String] = Nil,
                                 reference_genome: Option[String] = None,
                                 repository: Option[String] = None,
                                 sequencing_experiments: Seq[SequencingExperiment_ES] = Nil,
                                 is_harmonized: Option[Boolean] = None,
                                 latest_did: Option[String] = None
                               )
