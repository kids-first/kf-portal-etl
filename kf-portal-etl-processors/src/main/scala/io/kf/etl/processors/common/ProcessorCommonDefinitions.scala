package io.kf.etl.processors.common

import io.kf.etl.external.dataservice.entity._
import io.kf.etl.external.hpo.{GraphPath, OntologyTerm}
import org.apache.spark.sql.Dataset

object ProcessorCommonDefinitions {

  object DataServiceEntityNames extends Enumeration {
    val Participant = Value("participant")
    val Family = Value("family")
    val Biospecimen = Value("biospecimen")
    val Biospecimen_Genomic_File = Value("biospecimen_genomic_file")
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
    biospecimenGenomicFiles: Dataset[EBiospecimenGenomicFile],
    investigators: Dataset[EInvestigator],
    outcomes: Dataset[EOutcome],
    phenotypes: Dataset[EPhenotype],
    sequencingExperiments: Dataset[ESequencingExperiment],
    sequencingExperimentGenomicFiles: Dataset[ESequencingExperimentGenomicFile],
    studies: Dataset[EStudy],
    studyFiles: Dataset[EStudyFile],
    ontologyData: OntologiesDataSet
  )

  case class EntityEndpointSet(
    participants: Seq[String],
    families: Seq[String],
    biospecimens: Seq[String],
    diagnoses: Seq[String],
    familyRelationships: Seq[String],
    genomicFiles: Seq[String],
    biospecimenGenomicFiles: Seq[String],
    investigators: Seq[String],
    outcomes: Seq[String],
    phenotypes: Seq[String],
    sequencingExperiments: Seq[String],
    sequencingExperimentGenomicFiles: Seq[String],
    studies: Seq[String],
    studyFiles: Seq[String]
  )

  case class OntologiesDataSet(
    hpoGraphPath: Dataset[GraphPath],
    hpoTerms: Dataset[OntologyTerm],
    mondoTerms: Dataset[OntologyTerm],
    ncitTerms: Dataset[OntologyTerm]
  )
}
