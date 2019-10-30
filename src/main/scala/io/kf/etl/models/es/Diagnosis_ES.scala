package io.kf.etl.models.es

final case class Diagnosis_ES(
                               age_at_event_days: scala.Option[Int] = None,
                               diagnosis_category: scala.Option[String] = None,
                               external_id: scala.Option[String] = None,
                               icd_id_diagnosis: scala.Option[String] = None,
                               kf_id: scala.Option[String] = None,
                               mondo_id_diagnosis: scala.Option[String] = None,
                               source_text_diagnosis: scala.Option[String] = None,
                               uberon_id_tumor_location: scala.Option[String] = None,
                               source_text_tumor_location: scala.Option[String] = None,
                               ncit_id_diagnosis: scala.Option[String] = None,
                               spatial_descriptor: scala.Option[String] = None,
                               diagnosis: scala.Option[String] = None,
                               biospecimens: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty
                             )
