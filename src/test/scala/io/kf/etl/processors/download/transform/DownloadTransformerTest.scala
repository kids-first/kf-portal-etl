package io.kf.etl.processors.download.transform

import io.kf.etl.models.dataservice.{EDiagnosis, EParticipant, EStudy}
import io.kf.etl.models.duocode.DuoCode
import io.kf.etl.models.ontology.{OntologyTerm, OntologyTermBasic}
import io.kf.etl.processors.test.util.EntityUtil.{buildEntityDataSet, buildOntologiesDataSet}
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}


class DownloadTransformerTest extends FlatSpec with Matchers with WithSparkSession {
  import spark.implicits._
  "loadTerms" should "load ontological terms from compressed TSV file" in {
    val terms = spark.read.json("./src/test/resources/mondo_terms.json").select("id", "name", "parents", "ancestors", "is_leaf").as[OntologyTerm]


    terms.collect() should contain theSameElementsAs Seq(
      OntologyTerm(
        id = "MONDO:0015341",
        name = "congenital panfollicular nevus (disease)",
        parents = Seq("melanocytic nevus (MONDO:0005073)"),
        ancestors = Seq(
          OntologyTermBasic(
            id = "MONDO:0005070",
            name = "neoplasm (disease)",
            parents = Seq("neoplastic disease or syndrome (MONDO:0023370)"))
        ),
        is_leaf = true),
      OntologyTerm(id = "MONDO:0007471", name = "Doyne honeycomb retinal dystrophy", is_leaf = true)
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
      EDiagnosis(kf_id = Some("diagnosis_1"), mondo_id_diagnosis = Some("MONDO:0005072"), ncit_id_diagnosis = Some("NCIT:C0475358"), source_text_diagnosis = Some("Neuroblastoma source text")),
      EDiagnosis(kf_id = Some("diagnosis_2"), ncit_id_diagnosis = Some("NCIT:C0475358"), source_text_diagnosis = Some("Neuroblastoma source text")),
      EDiagnosis(kf_id = Some("diagnosis_3"), source_text_diagnosis = Some("Neuroblastoma source text")),
      EDiagnosis(kf_id = Some("diagnosis_4")),
      EDiagnosis(kf_id = Some("diagnosis_5"), mondo_id_diagnosis = Some("MONDO:UNKNOWN"), ncit_id_diagnosis = Some("NCIT:C0475358"), source_text_diagnosis = Some("Neuroblastoma source text")),
      EDiagnosis(kf_id = Some("diagnosis_6"), ncit_id_diagnosis = Some("NCIT:UNKNOWN"), source_text_diagnosis = Some("Neuroblastoma source text"))
    )
    val ontologiesDataset = buildOntologiesDataSet(
      ncitTerms = Seq(
        OntologyTermBasic(name = "Neuroblastoma NCIT", id = "NCIT:C0475358"),
        OntologyTermBasic(name = "Ewing Sarcoma NCIT", id = "NCIT:C14165")
      ),
      mondoTerms = Seq(
        OntologyTerm(name = "Neuroblastoma Mondo", id = "MONDO:0005072"),
        OntologyTerm(name = "Ewing Sarcoma Mondo", id = "MONDO:0005073")
      )

    )

    val result = DownloadTransformer.createDiagnosis(diagnoses, ontologiesDataset, spark).collect()

    result should contain theSameElementsAs Seq(
      EDiagnosis(kf_id = Some("diagnosis_1"), mondo_id_diagnosis = Some("MONDO:0005072"), ncit_id_diagnosis = Some("Neuroblastoma NCIT (NCIT:C0475358)"), source_text_diagnosis = Some("Neuroblastoma source text"), diagnosis_text = Some("Neuroblastoma Mondo")),
      EDiagnosis(kf_id = Some("diagnosis_2"), ncit_id_diagnosis = Some("Neuroblastoma NCIT (NCIT:C0475358)"), source_text_diagnosis = Some("Neuroblastoma source text"), diagnosis_text = Some("Neuroblastoma NCIT")),
      EDiagnosis(kf_id = Some("diagnosis_3"), source_text_diagnosis = Some("Neuroblastoma source text"), diagnosis_text = Some("Neuroblastoma source text")),
      EDiagnosis(kf_id = Some("diagnosis_4")),
      EDiagnosis(kf_id = Some("diagnosis_5"), ncit_id_diagnosis = Some("Neuroblastoma NCIT (NCIT:C0475358)"), source_text_diagnosis = Some("Neuroblastoma source text"), diagnosis_text = Some("Neuroblastoma NCIT")),
      EDiagnosis(kf_id = Some("diagnosis_6"), source_text_diagnosis = Some("Neuroblastoma source text"), diagnosis_text = Some("Neuroblastoma source text"))
    )

  }

  "createStudies" should "return enriched studies with extra parameters" in {
    val study1 =  EStudy(
      kf_id = Some("SD_46SK55A3"),
      name = Some("Kids First: Congenital Diaphragmatic Hernia"),
      visible = Some(true)
    )

    val study2 =  EStudy(
      kf_id = Some("SD_ZXJFFMEF"),
      name = Some("Kids First: Osteosarcoma"),
      visible = Some(true)
    )

    val study3 =  EStudy(
      kf_id = Some("SD_FUNNYSTUDY"),
      name = Some("No Extra Params"),
      visible = Some(true)
    )

    val study4 =  EStudy(
      kf_id = Some("SD_JWS3V24D"),
      name = Some("Kids First: Genetics at the Intersection of Childhood Cancer and Birth Defects"),
      visible = Some(true)
    )

    val studies = Seq(study1, study2, study3, study4)

    //"https://kf-qa-etl-bucket.s3.amazonaws.com/mapping/studies_short_name.tsv"
    val studiesExtraParamsDS = DownloadTransformer.studiesExtraParams("./src/test/resources/studies_short_name.tsv")(spark)

    val result = DownloadTransformer.createStudies(studies, studiesExtraParamsDS)(spark)

    result.collect() should contain theSameElementsAs Seq(
      EStudy(
        kf_id = Some("SD_ZXJFFMEF"),
        name = Some("Kids First: Osteosarcoma"),
        code= Some("KF-OS"),
        domain= Seq("Cancer"),
        program= Some("Kids First"),
        visible = Some(true)
      ),
      EStudy(
        kf_id = Some("SD_46SK55A3"),
        name = Some("Kids First: Congenital Diaphragmatic Hernia"),
        code= Some("KF-CDH"),
        domain= Seq("Birth Defect"),
        program= Some("Kids First"),
        visible = Some(true)
      ),
      EStudy(
        kf_id = Some("SD_FUNNYSTUDY"),
        name = Some("No Extra Params"),
        code= None,
        domain= Nil,
        program= None,
        visible = Some(true)
      ),
      EStudy(
        kf_id = Some("SD_JWS3V24D"),
        name = Some("Kids First: Genetics at the Intersection of Childhood Cancer and Birth Defects"),
        code= Some("KF-GNINT"),
        domain= Seq("Cancer", "Birth Defect"),
        program= Some("Kids First"),
        visible = Some(true)
      )
    )
  }
}
