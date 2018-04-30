package io.kf.etl.processors.common

import io.kf.etl.external.dataservice.entity._
import io.kf.etl.external.hpo.GraphPath
import org.apache.spark.sql.Dataset

object ProcessorCommonDefinitions {

  object DataServiceEntityNames extends Enumeration {
    val Participant = Value("participant")
    val Family = Value("family")
    val Biospecimen = Value("biospecimen")
    val Investigator = Value("investigator")
    val Study = Value("study")
    val Sequencing_Experiment = Value("sequencing_experiment")
    val Diagnosis = Value("diagnosis")
    val Phenotype = Value("phenotype")
    val Outcome = Value("outcome")
    val Genomic_File = Value("genomic_file")
    val Family_Relationship = Value("family_relationship")
    val Study_File = Value("study_file")
  }

  case class EntityDataSet(
    participants: Dataset[EParticipant],
    families: Dataset[EFamily],
    biospecimens: Dataset[EBiospecimen],
    diagnoses: Dataset[EDiagnosis],
    familyRelationships: Dataset[EFamilyRelationship],
    genomicFiles: Dataset[EGenomicFile],
    investigators: Dataset[EInvestigator],
    outcomes: Dataset[EOutcome],
    phenotypes: Dataset[EPhenotype],
    sequencingExperiments: Dataset[ESequencingExperiment],
    studies: Dataset[EStudy],
    studyFiles: Dataset[EStudyFile],
    graphPath: Dataset[GraphPath]
  )

  case class EntityEndpointSet(
    participants: String,
    families: String,
    biospecimens: String,
    diagnoses: String,
    familyRelationships: String,
    genomicFiles: String,
    investigators: String,
    outcomes: String,
    phenotypes: String,
    sequencingExperiments: String,
    studies: String,
    studyFiles: String
  )

}
