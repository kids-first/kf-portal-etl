package io.kf.etl.processor.common

import io.kf.etl.dbschema._
import io.kf.etl.model.Participant
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
     participantAlis: DS_PARTICIPANTALIAS,
     workflowGenomicFile: DS_WORKFLOWGENOMICFILE
  )

  object DBTables extends Enumeration{
    val Participant, Study, Demographic, Sample, Aliquot, SequencingExperiment, Diagnosis, Phenotype, Outcome, GenomicFile, Workflow, FamilyRelationship, ParticipantAlias, WorkflowGenomicFile = Value
  }

  type RelativeId = String
  type RelativeToParticipantRelation = Option[String]

  case class ParticipantDataTypes(kfId:String, datatypes:Seq[String])

  case class FamilyMemberRelation(kfId:String, relative: Participant, relation: RelativeToParticipantRelation)

}
