package io.kf.etl.processors.common.step.impl

import io.kf.etl.external.dataservice.entity.{EBiospecimen, EBiospecimenGenomicFile, EDiagnosis, EFamily, EFamilyRelationship, EGenomicFile, EInvestigator, EOutcome, EParticipant, EPhenotype, ESequencingExperiment, ESequencingExperimentGenomicFile, EStudy, EStudyFile}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, OntologiesDataSet}
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class MergeFamilyTest extends FlatSpec with Matchers {


  "calculateAvailableDataTypes" should "return a map of datatypes by participant id" in {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val p1 = EParticipant(kfId = Some("participant_id_1"))
    val bioSpecimen1 = EBiospecimen(kfId=Some("biospecimen_id_1"), participantId = Some("participant_id_1"))
    val biospecimenGeniomicFile1 = EBiospecimenGenomicFile(kfId = Some("biospeciment_genomic_file_id_1"), biospecimenId = Some("biospecimen_id_1"), genomicFileId = Some("genomic_file_id_1"))
    val genomicFile1 = EGenomicFile(kfId=Some("genomic_file_id_1"), dataType = Some("datatype_1"))
    val entityDataset = EntityDataSet(
      participants = Seq(p1).toDS(),
      families = spark.emptyDataset[EFamily],
      biospecimens = Seq(bioSpecimen1).toDS(),
      diagnoses = spark.emptyDataset[EDiagnosis],
      familyRelationships = spark.emptyDataset[EFamilyRelationship],
      genomicFiles = Seq(genomicFile1).toDS(),
      biospecimenGenomicFiles = Seq(biospecimenGeniomicFile1).toDS(),
      investigators = spark.emptyDataset[EInvestigator],
      outcomes = spark.emptyDataset[EOutcome],
      phenotypes = spark.emptyDataset[EPhenotype],
      sequencingExperiments = spark.emptyDataset[ESequencingExperiment],
      sequencingExperimentGenomicFiles = spark.emptyDataset[ESequencingExperimentGenomicFile],
      studies = spark.emptyDataset[EStudy],
      studyFiles = spark.emptyDataset[EStudyFile],
      ontologyData = null
    )

    val mergeFamily = new MergeFamily(
      ctx = StepContext(
        spark = spark,
        processorName = "Test Merge Family",
        processorDataPath = "",
        hdfs = null,
        entityDataset = entityDataset
      )
    )

    mergeFamily.calculateAvailableDataTypes(entityDataset).value shouldBe Map("participant_id_1" -> Seq("datatype_1"))

  }
}
