package io.kf.etl.processors.common

import io.kf.etl.dbschema._
import io.kf.etl.model._
import io.kf.etl.model.utils.{GenomicFileToStudy, ParticipantToGenomicFiles, TransformedGraphPath}
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
  type DS_PARTICIPANTALIAS = Dataset[TParticipantAlias]
  type DS_WORKFLOWGENOMICFILE = Dataset[TWorkflowGenomicFile]
  type DS_GRAPHPATH = Dataset[TransformedGraphPath]
  type DS_PARTICIPANT_GENOMICFILE = Dataset[ParticipantToGenomicFiles]
  type DS_GENOMICFILE_STUDY = Dataset[GenomicFileToStudy]

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
     workflowGenomicFile: DS_WORKFLOWGENOMICFILE,
//     participantAlis: DS_PARTICIPANTALIAS,
     graphPath: DS_GRAPHPATH,
     participantGenomicFile: DS_PARTICIPANT_GENOMICFILE,
     genomicFileToStudy: DS_GENOMICFILE_STUDY
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
//    val Participant_Alias = Value("participant_alias")
    val Workflow_Genomic_File = Value("workflow_genomic_file")

  }

}
