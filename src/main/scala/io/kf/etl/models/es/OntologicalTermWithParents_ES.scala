package io.kf.etl.models.es

final case class OntologicalTermWithParents_ES(
                                                name: String,
                                                parents: Seq[String] = Nil,
                                                age_at_event_days: Set[Int] = Set.empty[Int],
                                                is_leaf: Boolean = false,
                                                is_tagged: Boolean = false
                                         ) extends Ordered[OntologicalTermWithParents_ES] {


  override def compare(that: OntologicalTermWithParents_ES): Int = this.extractId compare that.extractId


  def extractId: String = {
    val regex(id) = name
    id
  }
  val regex = ".* \\([A-Z]+:([0-9]+)\\)".r //FIXME for mondo terms
}
