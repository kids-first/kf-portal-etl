package io.kf.etl.processor.filecentric.source

import java.util.Formatter

import io.kf.etl.common.Constants.HPO_GRAPH_PATH
import io.kf.etl.processor.common.ProcessorCommonDefinitions.{DatasetsFromDBTables, TransformedGraphPath}
import io.kf.etl.processor.filecentric.context.DocumentContext
import io.kf.etl.processor.repo.Repository
import io.kf.etl.dbschema._
import io.kf.etl.processor.common.ProcessorCommonDefinitions.PostgresqlDBTables._


class FileCentricSource(val context: DocumentContext) {

  def source(repo:Repository): DatasetsFromDBTables = {
    import context.sparkSession.implicits._

    val input_path =
      repo.url.getProtocol match {
        case "file" => repo.url.getFile
        case _ => repo.url.toString
      }

      val study = context.sparkSession.read.parquet(s"${input_path}/${Study.toString}").as[TStudy].cache()
      val participant = context.sparkSession.read.parquet(s"${input_path}/${Participant.toString}").as[TParticipant].cache()
      val demographic = context.sparkSession.read.parquet(s"${input_path}/${Demographic.toString}").as[TDemographic].cache()
      val sample = context.sparkSession.read.parquet(s"${input_path}/${Sample.toString}").as[TSample].cache()
      val aliquot = context.sparkSession.read.parquet(s"${input_path}/${Aliquot.toString}").as[TAliquot].cache()
      val sequencing_experiment = context.sparkSession.read.parquet(s"${input_path}/${Sequencing_Experiment.toString}").as[TSequencingExperiment].cache()
      val diagnosis = context.sparkSession.read.parquet(s"${input_path}/${Diagnosis.toString}").as[TDiagnosis].cache()
      val phenotype = context.sparkSession.read.parquet(s"${input_path}/${Phenotype.toString}").as[TPhenotype].cache()
      val genomic_file = context.sparkSession.read.parquet(s"${input_path}/${Genomic_File.toString}").as[TGenomicFile].cache()
      val family_relationship = context.sparkSession.read.parquet(s"${input_path}/${Family_Relationship.toString}").as[TFamilyRelationship].cache()
      val hpo_graph_path = context.sparkSession.read.parquet(s"${input_path}/${HPO_GRAPH_PATH}").as[TransformedGraphPath].cache()

    DatasetsFromDBTables(
      study,
      participant,
      demographic,
      sample,
      aliquot,
      sequencing_experiment,
      diagnosis,
      phenotype,
      null,
      genomic_file,
      null,
      family_relationship,
      null,
      hpo_graph_path
    )

//    DatasetsFromDBTables(
//      context.sparkSession.read.parquet(s"${input_path}/${Study.toString}").as[TStudy].cache(),
//      context.sparkSession.read.parquet(s"${input_path}/${Participant.toString}").as[TParticipant].cache(),
//      context.sparkSession.read.parquet(s"${input_path}/${Demographic.toString}").as[TDemographic].cache(),
//      context.sparkSession.read.parquet(s"${input_path}/${Sample.toString}").as[TSample].cache(),
//      context.sparkSession.read.parquet(s"${input_path}/${Aliquot.toString}").as[TAliquot].cache(),
//      context.sparkSession.read.parquet(s"${input_path}/${Sequencing_Experiment.toString}").as[TSequencingExperiment].cache(),
//      context.sparkSession.read.parquet(s"${input_path}/${Diagnosis.toString}").as[TDiagnosis].cache(),
//      context.sparkSession.read.parquet(s"${input_path}/${Phenotype.toString}").as[TPhenotype].cache(),
//      null,
////      context.sparkSession.read.parquet(s"${input_path}/${Outcome.toString}").as[TOutcome].cache(),
//      context.sparkSession.read.parquet(s"${input_path}/${Genomic_File.toString}").as[TGenomicFile].cache(),
//      null,
////      context.sparkSession.read.parquet(s"${input_path}/${Workflow.toString}").as[TWorkflow].cache(),
//      context.sparkSession.read.parquet(s"${input_path}/${Family_Relationship.toString}").as[TFamilyRelationship].cache(),
//      null,
////      context.sparkSession.read.parquet(s"${input_path}/${Workflow_Genomic_File.toString}").as[TWorkflowGenomicFile].cache(),
//      context.sparkSession.read.parquet(s"${input_path}/${HPO_GRAPH_PATH}").as[TransformedGraphPath].cache()
//    )
  }

}
