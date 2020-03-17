package io.kf.etl.models.es

final case class PhenotypeWithParents_ES (
                                           name: String,
                                           parents: Seq[String] = Nil,
                                           age_at_event_days: Set[Int] = Set.empty[Int],
                                           isLeaf: Boolean = false
                                         ) extends Ordered[PhenotypeWithParents_ES] {


  override def compare(that: PhenotypeWithParents_ES): Int = this.extractId compare that.extractId


  def extractId: String = {
    val regex(id) = name
    id
  }
  val regex = ".* \\(HP:([0-9]+)\\)".r
}
