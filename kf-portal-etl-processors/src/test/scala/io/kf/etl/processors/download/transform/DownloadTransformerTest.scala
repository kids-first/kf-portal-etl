package io.kf.etl.processors.download.transform

import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}


class DownloadTransformerTest extends FlatSpec with Matchers with WithSparkSession {

  "loadTerms" should "load ontological terms from comnpressed TSV file" in {
    val terms = DownloadTransformer.loadTerms(getClass.getResource("/mondo.tsv.gz").toString, spark)
    terms.collect() should contain theSameElementsAs Seq(
      OntologyTerm(id="MONDO:1234", name="This is a monddo term"),
      OntologyTerm(id="MONDO:5678", name="Another mondo term")
    )
  }
}
