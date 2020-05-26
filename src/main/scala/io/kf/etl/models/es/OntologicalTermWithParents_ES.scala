package io.kf.etl.models.es

final case class OntologicalTermWithParents_ES(
                                                name: String,
                                                parents: Seq[String] = Nil,
                                                age_at_event_days: Set[Int] = Set.empty[Int],
                                                is_leaf: Boolean = false,
                                                is_tagged: Boolean = false
                                         ) extends TermWithParents_ES with Ordered[OntologicalTermWithParents_ES]
{
  override def compare(that: OntologicalTermWithParents_ES): Int = this.extractId compare that.extractId
}
