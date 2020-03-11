package io.kf.etl.processors.test.util

import io.kf.etl.models.dataservice._
import io.kf.etl.models.duocode.DuoCode
import io.kf.etl.models.ontology.{HPOOntologyTerm, OntologyTerm}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, OntologiesDataSet}
import org.apache.spark.sql.{Dataset, SparkSession}

object EntityUtil {


  def buildEntityDataSet(
                          participants: Seq[EParticipant] = Nil,
                          families: Seq[EFamily] = Nil,
                          biospecimens: Seq[EBiospecimen] = Nil,
                          diagnoses: Seq[EDiagnosis] = Nil,
                          familyRelationships: Seq[EFamilyRelationship] = Nil,
                          genomicFiles: Seq[EGenomicFile] = Nil,
                          biospecimenGenomicFiles: Seq[EBiospecimenGenomicFile] = Nil,
                          biospecimenDiagnoses: Seq[EBiospecimenDiagnosis] = Nil,
                          investigators: Seq[EInvestigator] = Nil,
                          outcomes: Seq[EOutcome] = Nil,
                          phenotypes: Seq[EPhenotype] = Nil,
                          sequencingExperiments: Seq[ESequencingExperiment] = Nil,
                          sequencingExperimentGenomicFiles: Seq[ESequencingExperimentGenomicFile] = Nil,
                          studies: Seq[EStudy] = Nil,
                          studyFiles: Seq[EStudyFile] = Nil,
                          ontologyData: Option[OntologiesDataSet] = None,
                          duoCodes: Option[Dataset[DuoCode]] = None
                        )(implicit spark: SparkSession): EntityDataSet = {
    import spark.implicits._
    EntityDataSet(

      participants = participants.toDS(),
      families = families.toDS(),
      biospecimens = biospecimens.toDS(),
      diagnoses = diagnoses.toDS(),
      familyRelationships = familyRelationships.toDS(),
      genomicFiles = genomicFiles.toDS(),
      biospecimenGenomicFiles = biospecimenGenomicFiles.toDS(),
      biospecimenDiagnoses = biospecimenDiagnoses.toDS(),
      investigators = investigators.toDS(),
      outcomes = outcomes.toDS(),
      phenotypes = phenotypes.toDS(),
      sequencingExperiments = sequencingExperiments.toDS(),
      sequencingExperimentGenomicFiles = sequencingExperimentGenomicFiles.toDS(),
      studies = studies.toDS(),
      studyFiles = studyFiles.toDS(),
      ontologyData = ontologyData.getOrElse(buildOntologiesDataSet()),
      duoCodeDataSet = duoCodes.getOrElse(spark.emptyDataset[DuoCode])
    )
  }

  def buildOntologiesDataSet(hpoTerms: Seq[HPOOntologyTerm] = Nil,
                             mondoTerms: Seq[OntologyTerm] = Nil,
                             ncitTerms: Seq[OntologyTerm] = Nil)(implicit spark: SparkSession): OntologiesDataSet = {
    import spark.implicits._
    OntologiesDataSet(hpoTerms.toDS(), mondoTerms.toDS(), ncitTerms.toDS())
  }


}
