package io.kf.etl.models.es

case class Family_ES(
                      family_id: scala.Option[String] = None,
                      family_compositions: _root_.scala.collection.Seq[FamilyComposition_ES] = _root_.scala.collection.Seq.empty,
                      father_id: scala.Option[String] = None,
                      mother_id: scala.Option[String] = None
                    )
