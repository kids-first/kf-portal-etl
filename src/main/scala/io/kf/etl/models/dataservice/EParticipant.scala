package io.kf.etl.models.dataservice

case class EParticipant(
                         affected_status: scala.Option[Boolean] = None,
                         alias_group: scala.Option[String] = None,
                         biospecimens: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                         created_at: scala.Option[String] = None,
                         diagnoses: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                         diagnosis_category: scala.Option[String] = None,
                         ethnicity: scala.Option[String] = None,
                         external_id: scala.Option[String] = None,
                         family_id: scala.Option[String] = None,
                         gender: scala.Option[String] = None,
                         is_proband: scala.Option[Boolean] = None,
                         kf_id: scala.Option[String] = None,
                         modified_at: scala.Option[String] = None,
                         outcomes: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                         phenotypes: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                         race: scala.Option[String] = None,
                         study_id: scala.Option[String] = None,
                         visible: scala.Option[Boolean] = None
    )
