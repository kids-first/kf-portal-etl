package io.kf.hpo.models.ontology

case class OntologyTerm (
                          id: String,
                          name: String,
                          parents: Seq[OntologyTerm] = Nil
                        ) {
  override def toString(): String = s"$name ($id)"
}

