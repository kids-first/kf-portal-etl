package io.kf.etl.processors.download.transform

import io.kf.etl.models.dataservice.EDiagnosis
import io.kf.etl.models.duocode.DuoCode
import io.kf.etl.models.ontology.OntologyTerm
import io.kf.etl.processors.test.util.EntityUtil.buildOntologiesDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}


class DownloadTransformerTest extends FlatSpec with Matchers with WithSparkSession {

  "loadTerms" should "load ontological terms from compressed TSV file" in {
    val terms = DownloadTransformer.loadTerms(getClass.getResource("/mondo.tsv.gz").toString, spark)
    terms.show(false)
    terms.collect() should contain theSameElementsAs Seq(
      OntologyTerm(id = "MONDO:1234", name = "This is a monddo term"),
      OntologyTerm(id = "MONDO:5678", name = "Another mondo term")
    )
  }

  "loadDuoLabel" should "load duo code and corresponding label from compressed CSV file" in {
    val duo = DownloadTransformer.loadDuoLabel(getClass.getResource("/duo").toString, spark)

    duo.collect().take(5) should contain theSameElementsAs Seq(
      DuoCode(id = "DUO:0000021", shorthand = Some("IRB"), Some("ethics approval required"), description = Some("This requirement indicates that the requestor must provide documentation of local IRB/ERB approval.")),
      DuoCode(id = "DUO:0000006", shorthand = Some("HMB"), Some("health or medical or biomedical research"), description = Some("This primary category consent code indicates that use is allowed for health/medical/biomedical purposes; does not include the study of population origins or ancestry.")),
      DuoCode(id = "DUO:0000019", shorthand = Some("PUB"), Some("publication required"), description = Some("This requirement indicates that requestor agrees to make results of studies using the data available to the larger scientific community.")),
      DuoCode(id = "DUO:0000026", shorthand = Some("US"), Some("user specific restriction"), description = Some("This requirement indicates that use is limited to use by approved users.")),
      DuoCode(id = "DUO:0000020", shorthand = Some("COL"), Some("collaboration required"), description = Some("This requirement indicates that the requestor must agree to collaboration with the primary study investigator(s)."))
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
