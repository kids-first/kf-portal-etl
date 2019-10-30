package io.kf.etl.models.es

final case class FileCentric_ES(
                                 acl: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                 access_urls: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                 availability: scala.Option[String] = None,
                                 controlled_access: scala.Option[Boolean] = None,
                                 data_type: scala.Option[String] = None,
                                 experiment_strategies: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                 external_id: scala.Option[String] = None,
                                 file_format: scala.Option[String] = None,
                                 file_name: scala.Option[String] = None,
                                 instrument_models: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                 is_paired_end: scala.Option[Boolean] = None,
                                 size: scala.Option[Long] = None,
                                 kf_id: scala.Option[String] = None,
                                 participants: _root_.scala.collection.Seq[Participant_ES] = _root_.scala.collection.Seq.empty,
                                 platforms: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                 reference_genome: scala.Option[String] = None,
                                 repository: scala.Option[String] = None,
                                 sequencing_experiments: _root_.scala.collection.Seq[SequencingExperiment_ES] = _root_.scala.collection.Seq.empty,
                                 is_harmonized: scala.Option[Boolean] = None,
                                 latest_did: scala.Option[String] = None
                               )
