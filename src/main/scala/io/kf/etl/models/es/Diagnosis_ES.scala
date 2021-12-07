package io.kf.etl.models.es

final case class Diagnosis_ES(
                               age_at_event_days: Option[Int] = None,
                               diagnosis_category: Option[String] = None,
                               external_id: Option[String] = None,
                               icd_id_diagnosis: Option[String] = None,
                               kf_id: Option[String] = None,
                               mondo_id_diagnosis: Option[String] = None,
                               source_text_diagnosis: Option[String] = None,
                               uberon_id_tumor_location: Option[String] = None,
                               source_text_tumor_location: Seq[String] = Seq.empty,
                               ncit_id_diagnosis: Option[String] = None,
                               spatial_descriptor: Option[String] = None,
                               diagnosis: Option[String] = None,
                               biospecimens: Seq[String] = Seq.empty,
                               is_tagged: Boolean = false,
                               mondo: Seq[DiagnosisTermWithParents_ES] = Seq.empty[DiagnosisTermWithParents_ES]
                             )

