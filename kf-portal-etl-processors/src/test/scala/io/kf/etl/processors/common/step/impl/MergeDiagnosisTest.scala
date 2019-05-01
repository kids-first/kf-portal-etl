package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Diagnosis_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EDiagnosis
import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.test.util.EntityUtil.{buildEntityDataSet, buildOntologiesDataSet}
import io.kf.etl.processors.test.util.StepContextUtil.buildContext
import io.kf.etl.processors.test.util.{StepContextUtil, WithSparkSession}
import org.scalatest.{FlatSpec, Matchers}

class MergeDiagnosisTest extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  "process" should "merge diagnoses and participnt and enrich diagnosis, mondo_diagnosis nd ncit diagnosis" in {
    val p1 = Participant_ES(kfId = Some("participant_id_1"))
    val diagnoses11 = EDiagnosis(kfId = Some("diagnosis_11"), participantId = Some("participant_id_1"), mondoIdDiagnosis = Some("MONDO:0005072"), ncitIdDiagnosis = Some("NCIT:C0475358"), sourceTextDiagnosis = Some("Neuroblastoma source text"))
    val diagnoses12 = EDiagnosis(kfId = Some("diagnosis_12"), participantId = Some("participant_id_1"), ncitIdDiagnosis = Some("NCIT:C0475358"), sourceTextDiagnosis = Some("Neuroblastoma source text"))
    val diagnoses13 = EDiagnosis(kfId = Some("diagnosis_13"), participantId = Some("participant_id_1"), sourceTextDiagnosis = Some("Neuroblastoma source text"))


    val p2 = Participant_ES(kfId = Some("participant_id_2"))
    val diagnoses21 = EDiagnosis(kfId = Some("diagnosis_21"), participantId = Some("participant_id_2"), mondoIdDiagnosis = Some("MONDO:0005072"))
    val diagnoses22 = EDiagnosis(kfId = Some("diagnosis_22"), participantId = Some("participant_id_2"), mondoIdDiagnosis = Some("MONDO:0005073"))

    val diagnoses3 = EDiagnosis(kfId = Some("diagnosis_3"))

    val p3 = Participant_ES(kfId = Some("participant_id_3"))

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

    val entityDataset = buildEntityDataSet(
      diagnoses = Seq(diagnoses11, diagnoses12, diagnoses13, diagnoses21, diagnoses22, diagnoses3),
      ontologyData = Some(ontologiesDataset)
    )

    val merge = new MergeDiagnosis(ctx = buildContext(entityDataset))

    val result = merge.process(Seq(p1, p2, p3).toDS()).collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(kfId = Some("participant_id_1"),
        diagnoses = Seq(
          Diagnosis_ES(kfId = Some("diagnosis_11"), diagnosis = Some("Neuroblastoma Mondo"), mondoIdDiagnosis = Some("Neuroblastoma Mondo (MONDO:0005072)"), ncitIdDiagnosis = Some("Neuroblastoma NCIT (NCIT:C0475358)"), sourceTextDiagnosis = Some("Neuroblastoma source text")),
          Diagnosis_ES(kfId = Some("diagnosis_12"), diagnosis = Some("Neuroblastoma NCIT"), ncitIdDiagnosis = Some("Neuroblastoma NCIT (NCIT:C0475358)"), sourceTextDiagnosis = Some("Neuroblastoma source text")),
          Diagnosis_ES(kfId = Some("diagnosis_13"), diagnosis = Some("Neuroblastoma source text"), sourceTextDiagnosis = Some("Neuroblastoma source text"))
        )
      ),
      Participant_ES(kfId = Some("participant_id_2"),
        diagnoses = Seq(
          Diagnosis_ES(kfId = Some("diagnosis_21"), diagnosis = Some("Neuroblastoma Mondo"), mondoIdDiagnosis = Some("Neuroblastoma Mondo (MONDO:0005072)")),
          Diagnosis_ES(kfId = Some("diagnosis_22"), diagnosis = Some("Ewing Sarcoma Mondo"), mondoIdDiagnosis = Some("Ewing Sarcoma Mondo (MONDO:0005073)"))
        )
      ),
      Participant_ES(kfId = Some("participant_id_3"))
    )
  }
}
