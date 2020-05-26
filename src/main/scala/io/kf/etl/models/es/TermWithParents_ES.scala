package io.kf.etl.models.es

trait TermWithParents_ES {
  val name: String
  val parents: Seq[String]
  val is_leaf: Boolean
  val is_tagged: Boolean

  val regex = ".* \\([A-Z]+:([0-9]+)\\)".r

  def extractId: String = {
    val regex(id) = name
    id
  }
}
