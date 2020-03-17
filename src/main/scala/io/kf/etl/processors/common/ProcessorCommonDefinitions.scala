package io.kf.etl.processors.common

import io.kf.etl.models.dataservice._
import io.kf.etl.models.duocode.DuoCode
import io.kf.etl.models.ontology.{OntologyTerm, OntologyTermBasic}
import org.apache.spark.sql.Dataset

object ProcessorCommonDefinitions {

  case class EntityDataSet(
                            participants: Dataset[EParticipant],
                            families: Dataset[EFamily],
                            biospecimens: Dataset[EBiospecimen],
                            diagnoses: Dataset[EDiagnosis],
                            familyRelationships: Dataset[EFamilyRelationship],
                            genomicFiles: Dataset[EGenomicFile],
                            biospecimenGenomicFiles: Dataset[EBiospecimenGenomicFile],
                            biospecimenDiagnoses:Dataset[EBiospecimenDiagnosis],
                            investigators: Dataset[EInvestigator],
                            outcomes: Dataset[EOutcome],
                            phenotypes: Dataset[EPhenotype],
                            sequencingExperiments: Dataset[ESequencingExperiment],
                            sequencingExperimentGenomicFiles: Dataset[ESequencingExperimentGenomicFile],
                            studies: Dataset[EStudy],
                            studyFiles: Dataset[EStudyFile],
                            ontologyData: OntologiesDataSet,
                            duoCodeDataSet: Dataset[DuoCode]
  )

  case class EntityEndpointSet(
                                participants: String,
                                families: String,
                                biospecimens: String,
                                diagnoses: String,
                                familyRelationships: String,
                                genomicFiles: String,
                                biospecimenGenomicFiles: String,
                                biospecimenDiagnoses: String,
                                investigators: String,
                                outcomes: String,
                                phenotypes: String,
                                sequencingExperiments: String,
                                sequencingExperimentGenomicFiles: String,
                                studies: String,
                                studyFiles: String
                              )

  case class OntologiesDataSet(
                                hpoTerms: Dataset[OntologyTerm],
                                mondoTerms: Dataset[OntologyTerm],
                                ncitTerms: Dataset[OntologyTermBasic]
                              )
}
