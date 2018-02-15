package io.kf.etl.processor.document.source

import io.kf.etl.processor.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processor.document.context.DocumentContext
import io.kf.etl.processor.repo.Repository
import io.kf.etl.dbschema._
import io.kf.etl.processor.common.ProcessorCommonDefinitions.DBTables._


class DocumentSource(val context: DocumentContext) {

  def source(repo:Repository): DatasetsFromDBTables = {
    import io.kf.etl.processor.datasource.KfHdfsParquetData._
    import context.sparkSession.implicits._
    DatasetsFromDBTables(
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${Study.toString}").as[TStudy].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${Participant.toString}").as[TParticipant].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${Demographic.toString}").as[TDemographic],
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${Sample.toString}").as[TSample].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${Aliquot.toString}").as[TAliquot].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${SequencingExperiment.toString}").as[TSequencingExperiment].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${Diagnosis.toString}").as[TDiagnosis].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${Phenotype.toString}").as[TPhenotype].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${Outcome.toString}").as[TOutcome].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${GenomicFile.toString}").as[TGenomicFile].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${Workflow.toString}").as[TWorkflow].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${FamilyRelationship.toString}").as[TFamilyRelationship].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${ParticipantAlias.toString}").as[TParticipantAlias].cache(),
      context.sparkSession.read.kfHdfsParquet(s"${repo.url.toString}/${WorkflowGenomicFile.toString}").as[TWorkflowGenomicFile].cache()
    )
  }

}
