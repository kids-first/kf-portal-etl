package io.kf.etl.models.es

final case class GenomicFile_ES(
                                 acl: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                 access_urls: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                 availability: scala.Option[String] = None,
                                 controlled_access: scala.Option[Boolean] = None,
                                 data_type: scala.Option[String] = None,
                                 external_id: scala.Option[String] = None,
                                 file_format: scala.Option[String] = None,
                                 file_name: scala.Option[String] = None,
                                 instrument_models: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                 is_harmonized: scala.Option[Boolean] = None,
                                 is_paired_end: scala.Option[Boolean] = None,
                                 kf_id: Option[String] = None,
                                 latest_did: scala.Option[String] = None,
                                 platforms: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                 reference_genome: scala.Option[String] = None,
                                 repository: scala.Option[String] = None,
                                 sequencing_experiments: Seq[SequencingExperiment_ES] = Nil,
                                 size: Option[Long] = None
                               )
