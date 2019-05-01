package io.kf.etl.processors.download.transform

import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.common.ontology.OwlManager
import org.scalatest.{FlatSpec, Matchers}

class OwlManagerTest extends FlatSpec with Matchers {

  "getOntologyTermsFromURL" should "return a map of term id -> term name for ncit file" in {
    val url = getClass.getClassLoader.getResource("ncit.xml")
    OwlManager.getOntologyTermsFromURL(url) shouldBe Seq(
      OntologyTerm(id="NCIT:C18009", name="Tumor Tissue"),
      OntologyTerm(id="NCIT:C43234", name="Not Reported")
    )

  }
  it should "return a map of term id -> term name for mondo file" in {
    val url = getClass.getClassLoader.getResource("mondo.xml")
    OwlManager.getOntologyTermsFromURL(url) shouldBe Seq(
      OntologyTerm(id="MONDO:0005072", name="neuroblastoma"),
      OntologyTerm(id="MONDO:0005073", name="melanocytic nevus")
    )

  }
}
