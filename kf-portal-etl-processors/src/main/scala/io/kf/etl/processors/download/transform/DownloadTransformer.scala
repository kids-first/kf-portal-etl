package io.kf.etl.processors.download.transform

import io.kf.etl.processors.common.ProcessorCommonDefinitions.{DatasetsFromDBTables, EntityDataSet, EntityEndpointSet}
import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.processors.repo.Repository
import io.kf.etl.processors.common.ProcessorCommonDefinitions.PostgresqlDBTables._
import io.kf.etl.dbschema._
import io.kf.etl.common.Constants._
import io.kf.etl.external.dataservice.entity._
import io.kf.etl.model.utils.TransformedGraphPath
import io.kf.etl.processors.download.source.EntityDataRetrieval
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.JsonAST.JValue



class DownloadTransformer(val context:DownloadContext) {

  def transform(repo: Repository): DatasetsFromDBTables = {

    import context.sparkSession.implicits._
    implicit val repository = repo

    DatasetsFromDBTables(
      generateDataset(Study.toString).map(row2TStudy),
      generateDataset(Participant.toString).map(row2Participant),
      generateDataset(Demographic.toString).map(row2Demographic),
      generateDataset(Sample.toString).map(row2Sample),
      generateDataset(Aliquot.toString).map(row2Aliquot),
      generateDataset(Sequencing_Experiment.toString).map(row2SequencingExperiment),
      generateDataset(Diagnosis.toString).map(row2Diagnosis),
      generateDataset(Phenotype.toString).map(row2Phenotype),
      generateDataset(Outcome.toString).map(row2Outcome),
      generateDataset(Genomic_File.toString).map(row2GenomicFile),
      generateDataset(Workflow.toString).map(row2Workflow),
      generateDataset(Family_Relationship.toString).map(row2FamilyRelationship),
      generateDataset(Workflow_Genomic_File.toString).map(row2WorkflowGenomicFile),
//      generateDataset(Participant_Alias.toString).map(row2ParticipantAlias),
      generateDataset(HPO_GRAPH_PATH).map(row2GraphPath),
      null, // participant => genomic_file
      null // genomic_file => study
    )
  }

  def generateDataset(table: String)(implicit repo:Repository): DataFrame = {
    context.sparkSession.read.option("sep", "\t").csv(s"${repo.url.toString}/${table.toString}")
  }

  val row2TStudy: Row=>TStudy = row => {
    {
      TStudy(
        uuid = row.getString(0),
        createdAt = row.getString(1),
        modifiedAt = row.getString(2),
        dataAccessAuthority = row.getString(3) match {
          case null | "null" => None
          case value:String => Some(value)
        },
        externalId = row.getString(4) match {
          case null | "null" => None
          case value:String => Some(value)
        },
        version = row.getString(5) match {
          case null | "null" => None
          case value:String => Some(value)
        },
        name = row.getString(6) match {
          case null | "null" => None
          case value:String => Some(value)
        },
        attribution = row.getString(7) match {
          case null | "null" => None
          case value:String => Some(value)
        },
        investigatorId = row.getString(8) match {
          case null | "null" => None
          case value:String => Some(value)
        },
        kfId = row.getString(9)
      )
    }
  }

  val row2Participant:Row=>TParticipant = row => {
    TParticipant(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      externalId = row.getString(3) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      familyId = row.getString(4) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      isProband = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(
          if(value.trim.equals("t")) true else false
        )
      },
      consentType = row.getString(6) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      studyId = row.getString(7) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      kfId = row.getString(8)
    )
  }

  val row2Demographic:Row=>TDemographic = row => {
    TDemographic(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      externalId = row.getString(3) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      race = row.getString(4) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      ethnicity = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      gender = row.getString(6) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      participantId = row.getString(7),
      kfId = row.getString(8)
    )
  }

  val row2Sample: Row=>TSample = row => {
    TSample(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      externalId = row.getString(3) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      tissueType = row.getString(4) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      composition = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      anatomicalSite = row.getString(6) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      ageAtEventDays = row.getString(7) match {
        case null | "null" => None
        case value: String => Some(value.toLong)
      },
      tumorDescriptor = row.getString(8) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      participantId = row.getString(9),
      kfId = row.getString(10)
    )
  }

  val row2Aliquot: Row=>TAliquot = row => {
    TAliquot(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      externalId = row.getString(3) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      shipmentOrigin = row.getString(4) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      shipmentDestination = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      analyteType = row.getString(6) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      concentration = row.getString(7) match {
        case null | "null" => None
        case value:String => Some(value.toFloat)
      },
      volume = row.getString(8) match {
        case null | "null" => None
        case value:String => Some(value.toFloat)
      },
      shipmentDate = row.getString(9) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      sampleId = row.getString(10),
      kfId = row.getString(11)
    )
  }

  val row2SequencingExperiment: Row=>TSequencingExperiment = row => {
    TSequencingExperiment(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      externalId = row.getString(3) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      experimentDate = row.getString(4) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      experimentStrategy = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      center = row.getString(6) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      libraryName = row.getString(7) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      libraryStrand = row.getString(8) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      isPairedEnd = row.getString(9) match {
        case null | "null" => None
        case value:String => Some(
          if(value.trim().equals("t")) true else false
        )
      },
      platform = row.getString(10) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      instrumentModel = row.getString(11) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      maxInsertSize = row.getString(12) match {
        case null | "null" => None
        case value:String => Some(value.toLong)
      },
      meanInsertSize = row.getString(13) match {
        case null | "null" => None
        case value:String => Some(value.toDouble)
      },
      meanDepth = row.getString(14) match {
        case null | "null" => None
        case value:String => Some(value.toDouble)
      },
      totalReads = row.getString(15) match {
        case null | "null" => None
        case value:String => Some(value.toLong)
      },
      meanReadLength = row.getString(16) match {
        case null | "null" => None
        case value:String => Some(value.toLong)
      },
      aliquotId = row.getString(17),
      kfId = row.getString(18)
    )
  }

  val row2Diagnosis: Row=>TDiagnosis = row => {
    TDiagnosis(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      externalId = row.getString(3) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      diagnosis = row.getString(4) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      diagnosisCategory = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      tumorLocation = row.getString(6) match {
        case null | "null"  => None
        case value:String => Some(value)
      },
      ageAtEventDays = row.getString(7) match {
        case null | "null" => None
        case value:String => Some(value.toLong)
      },
      participantId = row.getString(8),
      kfId = row.getString(9)
    )
  }

  val row2Phenotype: Row=>TPhenotype = row => {
    TPhenotype(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      phenotype = row.getString(3) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      hpoId = row.getString(4) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      observed = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      ageAtEventDays = row.getString(6) match {
        case null | "null" => None
        case value:String => Some(value.toLong)
      },
      participantId = row.getString(7),
      kfId = row.getString(8)
    )
  }

  val row2Outcome: Row=>TOutcome = row => {
    TOutcome(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      vitalStatus = row.getString(3) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      diseaseRelated = row.getString(4) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      ageAtEventDays = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value.toLong)
      },
      participantId = row.getString(6),
      kfId = row.getString(7)
    )
  }

  val row2GenomicFile: Row=>TGenomicFile = row => {
    TGenomicFile(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      fileName = row.getString(3) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      dataType = row.getString(4) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      fileFormat = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      fileSize = row.getString(6) match {
        case null | "null" => None
        case value:String => Some(value.toLong)
      },
      fileUrl = row.getString(7) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      md5Sum = row.getString(8) match {
        case null | "null" => None
        case value:String => Some(value.replace("-", ""))
      },
      controlledAccess = row.getString(9) match {
        case null | "null" => None
        case value:String => Some(
          if(value.trim.equals("t")) true else false
        )
      },
      sequencingExperimentId = row.getString(10),
      kfId = row.getString(11)
    )
  }

  val row2Workflow: Row=>TWorkflow = row => {
    TWorkflow(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      taskId = row.getString(3) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      name = row.getString(4) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      githubUrl = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      kfId = row.getString(6)
    )
  }

  val row2FamilyRelationship: Row=>TFamilyRelationship = row => {
    TFamilyRelationship(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      participantId = row.getString(3),
      relativeId = row.getString(4),
      participantToRelativeRelation = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      relativeToParticipantRelation = row.getString(6) match {
        case null | "null" => None
        case value:String => Some(value)
      },
      kfId = row.getString(7)
    )
  }

//  val row2ParticipantAlias: Row=>TParticipantAlias = {
//    scalaPbJson4sParser.fromJson[](json)
//  }

  val row2WorkflowGenomicFile: Row=>TWorkflowGenomicFile = row => {
    TWorkflowGenomicFile(
      uuid = row.getString(0),
      createdAt = row.getString(1),
      modifiedAt = row.getString(2),
      genomicFileId = row.getString(3),
      workflowId = row.getString(4),
      isInput = row.getString(5) match {
        case null | "null" => None
        case value:String => Some(value.toBoolean)
      },
      kfId = row.getString(6)
    )
  }

  val row2GraphPath: Row=>TransformedGraphPath = row => {
    TransformedGraphPath(
      term1 = "HP:%07d".format(row.getString(0).toInt),
      term2 = "HP:%07d".format(row.getString(1).toInt),
      distance = row.getString(2).toInt
    )
  }


  def transform(endpoints: EntityEndpointSet): EntityDataSet = {
    val retrieval = EntityDataRetrieval(context.config.dataService.url)
    EntityDataSet(
      participants = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.participants))).map(json2Participant),
      families = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.families))).map(json2Family),
      biospecimens = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.biospecimens))).map(json2Biospecimen),
      diagnoses = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.diagnoses))).map(json2Diagnosis),
      familyRelationships = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.familyRelationships))).map(json2FamilyRelationship),
      genomicFiles = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.genomicFiles))).map(json2GenomicFile),
      investigators = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.investigators))).map(json2Investigator),
      outcomes = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.outcomes))).map(json2Outcome),
      phenotypes = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.phenotypes))).map(json2Phenotype),
      sequencingExperiments = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.sequencingExperiments))).map(json2SeqExp),
      studies = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.studies))).map(json2Study),
      studyFiles = context.sparkSession.createDataset(retrieval.retrieve(Some(endpoints.studyFiles))).map(json2StudyFile)
    )
  }

  val scalaPbJson4sParser = new com.trueaccord.scalapb.json.Parser(preservingProtoFieldNames = true)

  val json2Participant: JValue=>EParticipant = json => {

    scalaPbJson4sParser.fromJson[EParticipant](json)
  }

  val json2Family: JValue=>EFamily = json => {
    scalaPbJson4sParser.fromJson[EFamily](json)
  }

  val json2Biospecimen: JValue=>EBiospecimen = json => {
    scalaPbJson4sParser.fromJson[EBiospecimen](json)
  }
  val json2Diagnosis: JValue=>EDiagnosis = json => {
    scalaPbJson4sParser.fromJson[EDiagnosis](json)
  }

  val json2FamilyRelationship: JValue=>EFamilyRelationship = json => {
    scalaPbJson4sParser.fromJson[EFamilyRelationship](json)
  }

  val json2GenomicFile: JValue=>EGenomicFile = json => {
    scalaPbJson4sParser.fromJson[EGenomicFile](json)
  }

  val json2Investigator: JValue=>EInvestigator = json => {
    scalaPbJson4sParser.fromJson[EInvestigator](json)
  }

  val json2Outcome: JValue=>EOutcome = json => {
    scalaPbJson4sParser.fromJson[EOutcome](json)
  }

  val json2Phenotype: JValue=>EPhenotype = json => {
    scalaPbJson4sParser.fromJson[EPhenotype](json)
  }

  val json2SeqExp: JValue=>ESequencingExperiment = json => {
    scalaPbJson4sParser.fromJson[ESequencingExperiment](json)
  }

  val json2Study: JValue=>EStudy = json => {
    scalaPbJson4sParser.fromJson[EStudy](json)
  }

  val json2StudyFile: JValue=>EStudyFile = json => {
    scalaPbJson4sParser.fromJson[EStudyFile](json)
  }


}
