package io.kf.etl.models.es

final case class FamilyComposition_ES(
                                       composition: scala.Option[String] = None,
                                       shared_hpo_ids: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                       available_data_types: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
                                       family_members: _root_.scala.collection.Seq[FamilyMember_ES] = _root_.scala.collection.Seq.empty
                                     )
