package io.kf.hpo.models.ontology

case class OntologyTerm (
                          id: String,
                          name: String,
                          parents: Seq[OntologyTerm] = Nil,
                          is_leaf: Boolean = false
                        ) {
  override def toString(): String = s"$name ($id)"
}

