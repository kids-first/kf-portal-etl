package io.kf.etl.processors.download.transform

import com.typesafe.config.Config
import io.kf.etl.common.Constants._
import io.kf.etl.models.dataservice._
import io.kf.etl.models.duocode.DuoCode
import io.kf.etl.models.ontology.{OntologyTerm, OntologyTermBasic}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, EntityEndpointSet, OntologiesDataSet}
import io.kf.etl.processors.download.transform.DownloadTransformer._
import io.kf.etl.processors.download.transform.utils.{DataServiceConfig, EntityDataRetriever}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import play.api.libs.ws.StandaloneWSClient

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class DownloadTransformer(implicit WSClient: StandaloneWSClient, ec: ExecutionContext, spark: SparkSession, config: Config) {

  val dataService = DataServiceConfig(config)
  val filters: Seq[String] = Seq("visible=true")

  def downloadOntologyData(): OntologiesDataSet = {

    val mondoTerms = loadTerms(config.getString(CONFIG_NAME_MONDO_PATH), spark)
    val ncitTerms = loadTermsBasic(config.getString(CONFIG_NAME_NCIT_PATH), spark)
    val hpoTerms = loadTerms(config.getString(CONFIG_NAME_HPO_PATH), spark)
    OntologiesDataSet(
      hpoTerms = hpoTerms.cache(),
      mondoTerms = mondoTerms.cache(),
      ncitTerms = ncitTerms.cache()
    )
  }

  def downloadDuoCodeLabelMap(): Dataset[DuoCode] = {
    loadDuoLabel(config.getString(CONFIG_NAME_DUOCODE_PATH), spark).cache()
  }

  def setFileRepo(file: EGenomicFile): EGenomicFile = {
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

    import spark.implicits._

    val ontologyData = downloadOntologyData()
    val retriever = EntityDataRetriever(dataService, filters)
    val duoCodeDs = downloadDuoCodeLabelMap()

    val participantsF = retriever.retrieve[EParticipant](endpoints.participants)
    val familiesF = retriever.retrieve[EFamily](endpoints.families)
    val biospecimensF = retriever.retrieve[EBiospecimen](endpoints.biospecimens)
    val diagnosesF: Future[Seq[EDiagnosis]] = retriever.retrieve[EDiagnosis](endpoints.diagnoses)
    val biospecimenDiagnosesF = retriever.retrieve[EBiospecimenDiagnosis](endpoints.biospecimenDiagnoses)
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
      biospecimenDiagnoses <- biospecimenDiagnosesF
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
        biospecimenDiagnoses = spark.createDataset(biospecimenDiagnoses).cache,
        diagnoses = createDiagnosis(diagnoses, ontologyData, spark).cache,
        genomicFiles = spark.createDataset(genomicFiles)
          .filter(filterGenomicFile _)
          .cache(),
        studyFiles = spark.emptyDataset[EStudyFile],

        // following two (graphPath, hpoTerms) are read from HPO mysql db:
        ontologyData = ontologyData,
        duoCodeDataSet = duoCodeDs
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

  def loadTermsBasic(path: String, spark: SparkSession): Dataset[OntologyTermBasic] = {
    import spark.implicits._
    spark.sparkContext.addFile(path)
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true)
    )
    )
    val filename = path.split("/").last
    spark.read.option("sep", "\t").schema(schema).csv(SparkFiles.get(filename)).withColumn("parents", lit(null).cast(ArrayType(StringType))).as[OntologyTermBasic]
  }

  def loadTerms(path: String, spark: SparkSession): Dataset[OntologyTerm] = {
    import spark.implicits._
    spark.sparkContext.addFile(path)

    val filename = path.split("/").last
    spark.read.json(SparkFiles.get(filename)).as[OntologyTerm]
  }

  def loadDuoLabel(path: String, spark: SparkSession): Dataset[DuoCode] = {
    import spark.implicits._
    spark.sparkContext.addFile(path, recursive = true)
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("shorthand", StringType),
      StructField("label", StringType, nullable = false),
      StructField("description", StringType)
    ))
    val filename = path.split("/").last
    spark.read.option("sep", ",").option("header", value = true).schema(schema).csv(SparkFiles.get(filename)).as[DuoCode]
  }

  def createDiagnosis(diagnoses: Seq[EDiagnosis], ontology: OntologiesDataSet, spark: SparkSession): Dataset[EDiagnosis] = {

    import spark.implicits._
    val diagnosesDS = spark.createDataset(diagnoses)
    diagnosesDS
      .joinWith(ontology.mondoTerms, diagnosesDS("mondoIdDiagnosis") === ontology.mondoTerms("id"), "left_outer")
      .as[(EDiagnosis, Option[OntologyTermBasic])]
      .joinWith(ontology.ncitTerms, $"_1.ncitIdDiagnosis" === ontology.ncitTerms("id"), "left_outer")
      .map { case ((d, m), n) => (d, m, Option(n)) }
      .map {
        case (d, optMondoTerm, optNcitTerm) if optMondoTerm.isDefined => d.copy(diagnosisText = optMondoTerm.map(_.name), mondoIdDiagnosis = formatTerm(optMondoTerm), ncitIdDiagnosis = formatTerm(optNcitTerm))
        case (d, _, optNcitTerm) if optNcitTerm.isDefined => d.copy(diagnosisText = optNcitTerm.map(_.name), mondoIdDiagnosis = None, ncitIdDiagnosis = formatTerm(optNcitTerm))
        case (d, _, _) => d.copy(diagnosisText = d.sourceTextDiagnosis, mondoIdDiagnosis = None, ncitIdDiagnosis = None)
      }

  }


  def formatTerm(term: Option[OntologyTermBasic]): Option[String] = term.map(t => s"${t.name} (${t.id})")
}
