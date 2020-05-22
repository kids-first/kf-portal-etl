package io.kf.etl.models.es

case class DiagnosisTermWithParents_ES(
                                     name: String,
                                     parents: Seq[String] = Nil,
                                     is_leaf: Boolean = false,
                                     is_tagged: Boolean = false
                                   ) extends TermWithParents_ES
