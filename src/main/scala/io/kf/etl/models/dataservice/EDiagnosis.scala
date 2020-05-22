package io.kf.etl.models.dataservice

case class EDiagnosis(
                       age_at_event_days: Option[Int] = None,
                       created_at: Option[String] = None,
                       diagnosis_category: Option[String] = None,
                       external_id: Option[String] = None,
                       icd_id_diagnosis: Option[String] = None,
                       kf_id: Option[String] = None,
                       modified_at: Option[String] = None,
                       mondo_id_diagnosis: Option[String] = None,
                       participant_id: Option[String] = None,
                       source_text_diagnosis: Option[String] = None,
                       uberon_id_tumor_location: Option[String] = None,
                       source_text_tumor_location: Option[String] = None,
                       ncit_id_diagnosis: Option[String] = None,
                       spatial_descriptor: Option[String] = None,
                       diagnosis_text: Option[String] = None,
                       biospecimens: _root_.scala.collection.Seq[String] = Nil,
                       visible: Option[Boolean] = None
    ) extends ObservableAtAge
