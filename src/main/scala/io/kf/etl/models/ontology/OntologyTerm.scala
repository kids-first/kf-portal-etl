package io.kf.etl.models.ontology

case class HPOOntologyTerm(
                         id: String,
                         name: String,
                         parents: Seq[OntologyTerm] = Nil,
                         ancestors: Seq[OntologyTerm] = Nil,
                         is_leaf: Option[Boolean] = None
                       ){
  override def toString: String = {
    s"${name} ($id)"
  }
}

case class OntologyTerm(
                              id: String,
                              name: String
                            ){
  override def toString: String = {
    s"${name} ($id)"
  }
}