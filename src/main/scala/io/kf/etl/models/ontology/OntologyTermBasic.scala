package io.kf.etl.models.ontology

case class OntologyTerm(
                            id: String,
                            name: String,
                            parents: Seq[String] = Nil,
                            ancestors: Seq[OntologyTermBasic] = Nil,
                            isLeaf: Boolean = false
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