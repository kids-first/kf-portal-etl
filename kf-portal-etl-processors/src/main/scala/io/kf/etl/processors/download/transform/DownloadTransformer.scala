package io.kf.etl.processors.download.transform

import io.kf.etl.external.dataservice.entity._
import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, EntityEndpointSet, OntologiesDataSet}
import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.processors.download.transform.DownloadTransformer._
import io.kf.etl.processors.download.transform.hpo.HPOTerm
import io.kf.etl.processors.download.transform.utils.EntityDataRetriever
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import play.api.libs.ws.StandaloneWSClient

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class DownloadTransformer(val context: DownloadContext)(implicit WSClient: StandaloneWSClient, ec: ExecutionContext) {

  val filters: Seq[String] = Seq("visible=true")

  def downloadOntologyData(): OntologiesDataSet = {
    val spark = context.appContext.sparkSession

    val mondoTerms = loadTerms(context.config.mondoPath, spark)
    val ncitTerms = loadTerms(context.config.ncitPath, spark)
    OntologiesDataSet(
      hpoTerms = HPOTerm.get(context).cache,
      mondoTerms = mondoTerms.cache(),
      ncitTerms = ncitTerms.cache()
    )
  }


  def setFileRepo(file: EGenomicFile): EGenomicFile = {

    import context.appContext.dataService

    // Repository values are derived from the accessUrl
    // The host of the URL will match to "dcf" or "gen3", which are the repository values
    var repo: Option[String] = None
    file.accessUrls.headOption match {
      case Some(url) =>
        if (url.contains(dataService.dcfHost)) {
          repo = Some("dcf")
        } else if (url.contains(dataService.gen3Host)) {
          repo = Some("gen3")
        }

      case None =>
    }


    file.copy(
      repository = repo
    )
  }

  def setCavaticaIdForRepo(file: EGenomicFile): EGenomicFile = {

    file.repository match {
      case Some(repo) => repo match {
        case "dcf" => file.copy(latestDid = file.externalId)
        case "gen3" => file
        case _ => file
      }
      case None => file
    }
  }

  def updateFileRepositoryData(file: EGenomicFile): EGenomicFile = Function.chain(Seq(
    setFileRepo(_),
    setCavaticaIdForRepo(_)
  ))(file)


  def transform(endpoints: EntityEndpointSet): EntityDataSet = {

    import context.appContext.sparkSession.implicits._

    val spark = context.appContext.sparkSession

    val ontologyData = downloadOntologyData()
    val retriever = EntityDataRetriever(context.config.dataService, filters)

    val participantsF = retriever.retrieve[EParticipant](endpoints.participants)
    val familiesF = retriever.retrieve[EFamily](endpoints.families)
    val biospecimensF = retriever.retrieve[EBiospecimen](endpoints.biospecimens)
    val diagnosesF = retriever.retrieve[EDiagnosis](endpoints.diagnoses)
    val familyRelationshipsF = retriever.retrieve[EFamilyRelationship](endpoints.familyRelationships)
    val investigatorsF = retriever.retrieve[EInvestigator](endpoints.investigators)
    val outcomesF = retriever.retrieve[EOutcome](endpoints.outcomes)
    val phenotypesF = retriever.retrieve[EPhenotype](endpoints.phenotypes)
    val sequencingExperimentsF = retriever.retrieve[ESequencingExperiment](endpoints.sequencingExperiments)
    val sequencingExperimentGenomicFilesF = retriever.retrieve[ESequencingExperimentGenomicFile](endpoints.sequencingExperimentGenomicFiles)
    val studiesF = retriever.retrieve[EStudy](endpoints.studies)
    val biospecimenGenomicFilesF = retriever.retrieve[EBiospecimenGenomicFile](endpoints.biospecimenGenomicFiles)
    val genomicFilesF = retriever.retrieve[EGenomicFile](endpoints.genomicFiles).map(_.map(updateFileRepositoryData))

    val dataset = for {
      participants <- participantsF
      families <- familiesF
      biospecimens <- biospecimensF
      familyRelationships <- familyRelationshipsF
      investigators <- investigatorsF
      outcomes <- outcomesF
      phenotypes <- phenotypesF
      sequencingExperiments <- sequencingExperimentsF
      sequencingExperimentGenomicFiles <- sequencingExperimentGenomicFilesF
      studies <- studiesF
      biospecimenGenomicFiles <- biospecimenGenomicFilesF
      diagnoses <- diagnosesF
      genomicFiles <- genomicFilesF
    } yield
      EntityDataSet(
        participants = spark.createDataset(participants).cache,
        families = spark.createDataset(families).cache,
        biospecimens = spark.createDataset(biospecimens).cache,
        familyRelationships = spark.createDataset(familyRelationships).cache,
        investigators = spark.createDataset(investigators).cache,
        outcomes = spark.createDataset(outcomes).cache,
        phenotypes = spark.createDataset(phenotypes).cache,
        sequencingExperiments = spark.createDataset(sequencingExperiments).cache,
        sequencingExperimentGenomicFiles = spark.createDataset(sequencingExperimentGenomicFiles).cache,
        studies = spark.createDataset(studies).cache,
        biospecimenGenomicFiles = spark.createDataset(biospecimenGenomicFiles).cache,
        diagnoses = spark.createDataset(diagnoses).cache,
        genomicFiles = spark.createDataset(genomicFiles)
          .filter(filterGenomicFile _)
          .cache(),
        studyFiles = context.appContext.sparkSession.emptyDataset[EStudyFile],

        // following two (graphPath, hpoTerms) are read from HPO mysql db:
        ontologyData = ontologyData
      )


    Await.result(dataset, Duration.Inf)
  }

}

object DownloadTransformer {
  def filterGenomicFile(f: EGenomicFile): Boolean = {
    f.dataType match {
      case Some(data_type) =>
        !data_type.toLowerCase.split(' ').takeRight(1)(0).equals("index")
      case None => true
    }
  }

  def loadTerms(path: String, spark: SparkSession): Dataset[OntologyTerm] = {
    import spark.implicits._
    spark.sparkContext.addFile(path)
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true)
    )
    )
    val filename = path.split("/").last
    spark.read.option("sep", "\t").schema(schema).csv(SparkFiles.get(filename)).as[OntologyTerm]
  }

}
