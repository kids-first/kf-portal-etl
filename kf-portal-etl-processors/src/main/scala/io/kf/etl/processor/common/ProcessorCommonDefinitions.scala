package io.kf.etl.processor.common

import io.kf.etl.dbschema._
import io.kf.etl.model.{Participant, Sample, SequencingExperiment, Workflow}
import org.apache.spark.sql.Dataset

object ProcessorCommonDefinitions {
  type DS_STUDY = Dataset[TStudy]
  type DS_PARTICIPANT = Dataset[TParticipant]
  type DS_DEMOGRAPHIC = Dataset[TDemographic]
  type DS_SAMPLE = Dataset[TSample]
  type DS_ALIQUOT = Dataset[TAliquot]
  type DS_SEQUENCINGEXPERIMENT = Dataset[TSequencingExperiment]
  type DS_DIAGNOSIS = Dataset[TDiagnosis]
  type DS_PHENOTYPE = Dataset[TPhenotype]
  type DS_OUTCOME = Dataset[TOutcome]
  type DS_GENOMICFILE = Dataset[TGenomicFile]
  type DS_WORKFLOW = Dataset[TWorkflow]
  type DS_FAMILYRELATIONSHIP = Dataset[TFamilyRelationship]
//  type DS_PARTICIPANTALIAS = Dataset[TParticipantAlias]
  type DS_WORKFLOWGENOMICFILE = Dataset[TWorkflowGenomicFile]
  type DS_GRAPHPATH = Dataset[TGraphPath]

  case class DatasetsFromDBTables(
     study: DS_STUDY,
     participant: DS_PARTICIPANT,
     demographic: DS_DEMOGRAPHIC,
     sample: DS_SAMPLE,
     aliquot: DS_ALIQUOT,
     sequencingExperiment: DS_SEQUENCINGEXPERIMENT,
     diagnosis: DS_DIAGNOSIS,
     phenotype: DS_PHENOTYPE,
     outcome: DS_OUTCOME,
     genomicFile: DS_GENOMICFILE,
     workflow: DS_WORKFLOW,
     familyRelationship: DS_FAMILYRELATIONSHIP,
//     participantAlis: DS_PARTICIPANTALIAS,
     workflowGenomicFile: DS_WORKFLOWGENOMICFILE,
     graphPath: DS_GRAPHPATH
  )

  object PostgresqlDBTables extends Enumeration{
    val Participant = Value("participant")
    val Study = Value("study")
    val Demographic = Value("demographic")
    val Sample = Value("sample")
    val Aliquot = Value("aliquot")
    val Sequencing_Experiment = Value("sequencing_experiment")
    val Diagnosis = Value("diagnosis")
    val Phenotype = Value("phenotype")
    val Outcome = Value("outcome")
    val Genomic_File = Value("genomic_file")
    val Workflow = Value("workflow")
    val Family_Relationship = Value("family_relationship")
    /*ParticipantAlias = Value("")*/
    val Workflow_Genomic_File = Value("workflow_genomic_file")

  }

  type RelativeId = String
  type RelativeToParticipantRelation = Option[String]

  case class FamilyMemberRelation(kfId:String, relative: Participant, relation: RelativeToParticipantRelation)

  case class ParticipantToGenomicFiles(kfId:String, fielIds:Seq[String], dataTypes:Seq[String])

  case class ParticipantToSamples(kfId:String, samples:Seq[Sample])

  case class GenomicFileToSeqExps(kfId:String, exps: Seq[SequencingExperiment])

  case class GenomicFileToWorkflows(kfId:String, flows: Seq[Workflow])

  case class GenomicFileToParticipants(kfId:String, participants:Seq[Participant])

  case class HPOReference(term:String, ancestors: Seq[String])

}
