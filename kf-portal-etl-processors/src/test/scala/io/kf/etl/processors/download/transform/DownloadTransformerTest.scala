package io.kf.etl.processors.download.transform

import io.kf.etl.external.dataservice.entity.EDiagnosis
import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.test.util.EntityUtil.{buildEntityDataSet, buildOntologiesDataSet}
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}


class DownloadTransformerTest extends FlatSpec with Matchers with WithSparkSession {

  "loadTerms" should "load ontological terms from comnpressed TSV file" in {
    val terms = DownloadTransformer.loadTerms(getClass.getResource("/mondo.tsv.gz").toString, spark)
    terms.collect() should contain theSameElementsAs Seq(
      OntologyTerm(id = "MONDO:1234", name = "This is a monddo term"),
      OntologyTerm(id = "MONDO:5678", name = "Another mondo term")
    )
  }


  "createDiagnosis" should "return enriched diagnosis with ontlogy terms" in {

    val diagnoses = Seq(
      EDiagnosis(kfId = Some("diagnosis_1"), mondoIdDiagnosis = Some("MONDO:0005072"), ncitIdDiagnosis = Some("NCIT:C0475358"), sourceTextDiagnosis = Some("Neuroblastoma source text")),
      EDiagnosis(kfId = Some("diagnosis_2"), ncitIdDiagnosis = Some("NCIT:C0475358"), sourceTextDiagnosis = Some("Neuroblastoma source text")),
      EDiagnosis(kfId = Some("diagnosis_3"), sourceTextDiagnosis = Some("Neuroblastoma source text")),
      EDiagnosis(kfId = Some("diagnosis_4")),
      EDiagnosis(kfId = Some("diagnosis_5"), mondoIdDiagnosis = Some("MONDO:UNKNOWN"), ncitIdDiagnosis = Some("NCIT:C0475358"), sourceTextDiagnosis = Some("Neuroblastoma source text")),
      EDiagnosis(kfId = Some("diagnosis_6"), ncitIdDiagnosis = Some("NCIT:UNKNOWN"), sourceTextDiagnosis = Some("Neuroblastoma source text"))
    )
    val ontologiesDataset = buildOntologiesDataSet(
      ncitTerms = Seq(
        OntologyTerm(name = "Neuroblastoma NCIT", id = "NCIT:C0475358"),
        OntologyTerm(name = "Ewing Sarcoma NCIT", id = "NCIT:C14165")
      ),
      mondoTerms = Seq(
        OntologyTerm(name = "Neuroblastoma Mondo", id = "MONDO:0005072"),
        OntologyTerm(name = "Ewing Sarcoma Mondo", id = "MONDO:0005073")
      )

    )

    val result = DownloadTransformer.createDiagnosis(diagnoses, ontologiesDataset, spark).collect()

    result should contain theSameElementsAs Seq(
      EDiagnosis(kfId = Some("diagnosis_1"), mondoIdDiagnosis = Some("Neuroblastoma Mondo (MONDO:0005072)"), ncitIdDiagnosis = Some("Neuroblastoma NCIT (NCIT:C0475358)"), sourceTextDiagnosis = Some("Neuroblastoma source text"), diagnosisText = Some("Neuroblastoma Mondo")),
      EDiagnosis(kfId = Some("diagnosis_2"), ncitIdDiagnosis = Some("Neuroblastoma NCIT (NCIT:C0475358)"), sourceTextDiagnosis = Some("Neuroblastoma source text"), diagnosisText = Some("Neuroblastoma NCIT")),
      EDiagnosis(kfId = Some("diagnosis_3"), sourceTextDiagnosis = Some("Neuroblastoma source text"), diagnosisText = Some("Neuroblastoma source text")),
      EDiagnosis(kfId = Some("diagnosis_4")),
      EDiagnosis(kfId = Some("diagnosis_5"), ncitIdDiagnosis = Some("Neuroblastoma NCIT (NCIT:C0475358)"), sourceTextDiagnosis = Some("Neuroblastoma source text"), diagnosisText = Some("Neuroblastoma NCIT")),
      EDiagnosis(kfId = Some("diagnosis_6"), sourceTextDiagnosis = Some("Neuroblastoma source text"), diagnosisText = Some("Neuroblastoma source text"))
    )

  }
}
