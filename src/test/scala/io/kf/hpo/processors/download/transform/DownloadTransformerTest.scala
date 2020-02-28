package io.kf.hpo.processors.download.transform

import io.kf.hpo.models.ontology.OntologyTerm
import org.scalatest.{FlatSpec, Matchers}

class DownloadTransformerTest extends FlatSpec with Matchers  {

  val a0: OntologyTerm = OntologyTerm("A0", "NameA0") //root

  val a1: OntologyTerm = OntologyTerm("A1", "NameA1", parents = Seq(OntologyTerm("A0", "NameA0")))
  val a2: OntologyTerm = OntologyTerm("A2", "NameA2", parents = Seq(OntologyTerm("A0", "NameA0")))

  val a11: OntologyTerm = OntologyTerm("A11", "NameA11", parents = Seq(OntologyTerm("A1", "NameA1")))
  val a12: OntologyTerm = OntologyTerm("A12", "NameA12", parents = Seq(OntologyTerm("A1", "NameA1")))
  val a21: OntologyTerm = OntologyTerm("A21", "NameA21", parents = Seq(OntologyTerm("A2", "NameA2")))

  val a111: OntologyTerm = OntologyTerm("A111", "NameA111", parents = Seq(OntologyTerm("A11", "NameA11")))
  val a112: OntologyTerm = OntologyTerm("A112", "NameA112", parents = Seq(OntologyTerm("A11", "NameA11")))
  val a121: OntologyTerm = OntologyTerm("A121", "NameA121", parents = Seq(OntologyTerm("A12", "NameA12")))
  val a122: OntologyTerm = OntologyTerm("A122", "NameA122", parents = Seq(OntologyTerm("A12", "NameA12")))
  val a221: OntologyTerm = OntologyTerm("A211", "NameA211", parents = Seq(OntologyTerm("A21", "NameA21"), OntologyTerm("A12", "NameA12")))

  val seq: List[OntologyTerm] = List(a0, a1, a2, a11, a12, a21, a111, a112, a121, a122, a221)
  val data: Map[String, OntologyTerm] = seq map (i => i.id -> i) toMap

  "loadTerms" should "load ontological terms from compressed TSV file" in {
    val result = DownloadTransformer.transformOntologyData(data)

    result should contain theSameElementsAs Map(
//      OntologyTerm(id = "A0", name = "NameA0") -> Set(), //first term (root) is not computed
      OntologyTerm("A1", "NameA1", parents = Seq(OntologyTerm("A0", "NameA0"))) -> Set(OntologyTerm("A0", "NameA0")),
      OntologyTerm("A2", "NameA2", parents = Seq(OntologyTerm("A0", "NameA0"))) -> Set(OntologyTerm("A0", "NameA0")),
      OntologyTerm("A11", "NameA11", parents = Seq(OntologyTerm("A1", "NameA1")))-> Set(OntologyTerm("A1", "NameA1"), OntologyTerm("A0", "NameA0")),
      OntologyTerm("A12", "NameA12", parents = Seq(OntologyTerm("A1", "NameA1")))-> Set(OntologyTerm("A1", "NameA1"), OntologyTerm("A0", "NameA0")),
      OntologyTerm("A21", "NameA21", parents = Seq(OntologyTerm("A2", "NameA2")))-> Set(OntologyTerm("A2", "NameA2"), OntologyTerm("A0", "NameA0")),
      OntologyTerm("A111", "NameA111", parents = Seq(OntologyTerm("A11", "NameA11")))->
        Set(OntologyTerm("A1", "NameA1"), OntologyTerm("A11", "NameA11"), OntologyTerm("A0", "NameA0")),
      OntologyTerm("A112", "NameA112", parents = Seq(OntologyTerm("A11", "NameA11")))->
        Set(OntologyTerm("A1", "NameA1"), OntologyTerm("A11", "NameA11"), OntologyTerm("A0", "NameA0")),
      OntologyTerm("A121", "NameA121", parents = Seq(OntologyTerm("A12", "NameA12")))->
        Set(OntologyTerm("A1", "NameA1"), OntologyTerm("A12", "NameA12"), OntologyTerm("A0", "NameA0")),
      OntologyTerm("A122", "NameA122", parents = Seq(OntologyTerm("A12", "NameA12")))->
        Set(OntologyTerm("A1", "NameA1"), OntologyTerm("A12", "NameA12"), OntologyTerm("A0", "NameA0")),
      OntologyTerm("A211", "NameA211", parents = Seq(OntologyTerm("A21", "NameA21"), OntologyTerm("A12", "NameA12"))) ->
        Set(
          OntologyTerm("A2", "NameA2"),
          OntologyTerm("A21", "NameA21"),
          OntologyTerm("A0", "NameA0"),
          OntologyTerm("A12", "NameA12"),
          OntologyTerm("A1", "NameA1")
        )
    )
  }

}
