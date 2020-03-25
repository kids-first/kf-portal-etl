package io.kf.etl.models.ontology

case class OntologyTerm(
                         id: String,
                         name: String,
                         parents: Seq[String] = Nil,
                         ancestors: Seq[OntologyTermBasic] = Nil,
                         is_leaf: Boolean = false
                       ){
  override def toString: String = {
    s"$name ($id)"
  }
}

case class OntologyTermBasic(
                              id: String,
                              name: String,
                              parents: Seq[String] = Nil
                            ){
  override def toString: String = {
    s"$name ($id)"
  }
}