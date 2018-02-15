package io.kf.etl.processor.download.transform

import io.kf.etl.processor.common.ProcessorCommonDefinitions.{DBTables, DatasetsFromDBTables}
import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository
import io.kf.etl.processor.common.ProcessorCommonDefinitions.DBTables._
import io.kf.etl.dbschema._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}



class DownloadTransformer(val context:DownloadContext) {

  def transform(repo: Repository): DatasetsFromDBTables = {

    import context.sparkSession.implicits._
    implicit val repository = repo

    DatasetsFromDBTables(
      generateDataset(Study).map(row2TStudy),
      generateDataset(Participant).map(row2Participant),
      generateDataset(Demographic).map(row2Demographic),
      generateDataset(Sample).map(row2Sample),
      generateDataset(Aliquot).map(row2Aliquot),
      generateDataset(SequencingExperiment).map(row2SequencingExperiment),
      generateDataset(Diagnosis).map(row2Diagnosis),
      generateDataset(Phenotype).map(row2Phenotype),
      generateDataset(Outcome).map(row2Outcome),
      generateDataset(GenomicFile).map(row2GenomicFile),
      generateDataset(Workflow).map(row2Workflow),
      generateDataset(FamilyRelationship).map(row2FamilyRelationship),
      generateDataset(ParticipantAlias).map(row2ParticipantAlias),
      generateDataset(WorkflowGenomicFile).map(row2WorkflowGenomicFile)
    )
  }

  def generateDataset(table: DBTables.Value)(implicit repo:Repository): DataFrame = {
    context.sparkSession.read.option("sep", "\t").csv(s"${repo.url.toString}/${table.toString}")
  }

  val row2TStudy: Row=>TStudy = row => {
    {
      TStudy(
        kfId = row.getString(0),
        row.getString(1),
        row.getString(2),
        row.getString(3),
        row.getString(4) match {
          case "null" => None
          case value:String => Some(value)
        },
        row.getString(5) match {
          case "null" => None
          case value:String => Some(value)
        },
        row.getString(6) match {
          case "null" => None
          case value:String => Some(value)
        },
        row.getString(7) match {
          case "null" => None
          case value:String => Some(value)
        },
        row.getString(8) match {
          case "null" => None
          case value:String => Some(value)
        }
      )
    }
  }

  val row2Participant:Row=>TParticipant = row => {
    TParticipant(
      row.getString(0),
      row.getString(1),
      row.getString(2),
      row.getString(3)
    )
  }

  val row2Demographic:Row=>TDemographic = ???

  val row2Sample: Row=>TSample = ???

  val row2Aliquot: Row=>TAliquot = ???

  val row2SequencingExperiment: Row=>TSequencingExperiment = ???

  val row2Diagnosis: Row=>TDiagnosis = ???

  val row2Phenotype: Row=>TPhenotype = ???

  val row2Outcome: Row=>TOutcome = ???

  val row2GenomicFile: Row=>TGenomicFile = ???

  val row2Workflow: Row=>TWorkflow = ???

  val row2FamilyRelationship: Row=>TFamilyRelationship = ???

  val row2ParticipantAlias: Row=>TParticipantAlias = ???

  val row2WorkflowGenomicFile: Row=>TWorkflowGenomicFile = ???

}
