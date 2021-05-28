package io.kf.etl.processors.download.transform

import akka.actor.ActorSystem
import com.typesafe.config.Config
import io.kf.etl.common.Constants._
import io.kf.etl.models.dataservice._
import io.kf.etl.models.duocode.DuoCode
import io.kf.etl.models.internal.StudyExtraParams
import io.kf.etl.models.ontology.{OntologyTerm, OntologyTermBasic}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.{
  EntityDataSet,
  EntityEndpointSet,
  OntologiesDataSet
}
import io.kf.etl.processors.download.transform.DownloadTransformer._
import io.kf.etl.processors.download.transform.utils.{
  DataServiceConfig,
  EntityDataRetriever
}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{
  ArrayType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{Dataset, SparkSession}
import play.api.libs.ws.StandaloneWSClient

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class DownloadTransformer(implicit
    WSClient: StandaloneWSClient,
    ec: ExecutionContext,
    spark: SparkSession,
    config: Config,
    system: ActorSystem
) {

  val dataService = DataServiceConfig(config)
  val filters: Seq[String] = Seq("visible=true")

  def downloadOntologyData(): OntologiesDataSet = {

    val mondoTerms = loadTerms(config.getString(CONFIG_NAME_MONDO_PATH), spark)
    val ncitTerms =
      loadTermsBasic(config.getString(CONFIG_NAME_NCIT_PATH), spark)
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

  def downloadStudiesExtraParams(path: String): Dataset[StudyExtraParams] = {
    studiesExtraParams(path)(spark).cache()
  }

  def downloadDataCategory_availableDataTypes(
      path: String
  ): Dataset[(String, Seq[String])] = {
    loadCategory_ExistingDataTypes(path)(spark).cache()
  }

  def setFileRepo(file: EGenomicFile): EGenomicFile = {
    // Repository values are derived from the accessUrl
    // The host of the URL will match to "dcf" or "gen3", which are the repository values
    var repo: Option[String] = None
    file.access_urls.headOption match {
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
      case Some(repo) =>
        repo match {
          case "dcf"  => file.copy(latest_did = file.external_id)
          case "gen3" => file
          case _      => file
        }
      case None => file
    }
  }

  def updateFileRepositoryData(file: EGenomicFile): EGenomicFile =
    Function.chain(
      Seq(
        setFileRepo(_),
        setCavaticaIdForRepo(_)
      )
    )(file)

  def transform(endpoints: EntityEndpointSet): EntityDataSet = {

    import spark.implicits._

    val ontologyData = downloadOntologyData()
    val studiesExtraParams = downloadStudiesExtraParams(
      config.getString(STUDIES_EXTRA_PARAMS_PATH)
    )
    val dataCategory_availableDataTypes =
      downloadDataCategory_availableDataTypes(
        config.getString(DATA_CAT_AVAILABLE_DATA_TYPES)
      )
    val retriever = EntityDataRetriever(dataService, filters)
    val duoCodeDs = downloadDuoCodeLabelMap()

    val participantsF = retriever.retrieve[EParticipant](endpoints.participants)
    val familiesF = retriever.retrieve[EFamily](endpoints.families)
    val biospecimensF = retriever.retrieve[EBiospecimen](endpoints.biospecimens)
    val diagnosesF: Future[Seq[EDiagnosis]] =
      retriever.retrieve[EDiagnosis](endpoints.diagnoses)
    val biospecimenDiagnosesF =
      retriever.retrieve[EBiospecimenDiagnosis](endpoints.biospecimenDiagnoses)
    val familyRelationshipsF =
      retriever.retrieve[EFamilyRelationship](endpoints.familyRelationships)
    val investigatorsF =
      retriever.retrieve[EInvestigator](endpoints.investigators)
    val outcomesF = retriever.retrieve[EOutcome](endpoints.outcomes)
    val phenotypesF = retriever.retrieve[EPhenotype](endpoints.phenotypes)
    val sequencingExperimentsF =
      retriever.retrieve[ESequencingExperiment](endpoints.sequencingExperiments)
    val sequencingExperimentGenomicFilesF =
      retriever.retrieve[ESequencingExperimentGenomicFile](
        endpoints.sequencingExperimentGenomicFiles
      )
    val studiesF = retriever.retrieve[EStudy](endpoints.studies)
    val biospecimenGenomicFilesF = retriever.retrieve[EBiospecimenGenomicFile](
      endpoints.biospecimenGenomicFiles
    )
    val genomicFilesF = retriever
      .retrieve[EGenomicFile](endpoints.genomicFiles)
      .map(_.map(updateFileRepositoryData))
    val sequencingCentersF =
      retriever.retrieve[ESequencingCenter](endpoints.sequencingCenters)

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
      sequencingCenters <- sequencingCentersF
    } yield EntityDataSet(
      participants = spark.createDataset(participants).cache,
      families = spark.createDataset(families).cache,
      biospecimens = spark.createDataset(biospecimens).cache,
      familyRelationships = spark.createDataset(familyRelationships).cache,
      investigators = spark.createDataset(investigators).cache,
      outcomes = spark.createDataset(outcomes).cache,
      phenotypes = spark.createDataset(phenotypes).cache,
      sequencingExperiments = spark.createDataset(sequencingExperiments).cache,
      sequencingExperimentGenomicFiles =
        spark.createDataset(sequencingExperimentGenomicFiles).cache,
      studies = createStudies(studies, studiesExtraParams)(spark).cache,
      biospecimenGenomicFiles =
        spark.createDataset(biospecimenGenomicFiles).cache,
      biospecimenDiagnoses = spark.createDataset(biospecimenDiagnoses).cache,
      diagnoses = createDiagnosis(diagnoses, ontologyData, spark).cache,
      genomicFiles = spark
        .createDataset(genomicFiles)
        .filter(filterGenomicFile _)
        .cache(),
      studyFiles = spark.emptyDataset[EStudyFile],
      // following two (graphPath, hpoTerms) are read from HPO mysql db:
      ontologyData = ontologyData,
      duoCodeDataSet = duoCodeDs,
      mapOfDataCategory_ExistingTypes = dataCategory_availableDataTypes,
      sequencingCenters = spark
        .createDataset(
          sequencingCenters
        )
        .cache
    )

    Await.result(dataset, Duration.Inf)
  }

}

object DownloadTransformer {
  def filterGenomicFile(f: EGenomicFile): Boolean = {
    f.data_type match {
      case Some(data_type) =>
        !data_type.toLowerCase.split(' ').takeRight(1)(0).equals("index")
      case None => true
    }
  }

  def loadTermsBasic(
      path: String,
      spark: SparkSession
  ): Dataset[OntologyTermBasic] = {
    import spark.implicits._
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable = true),
        StructField("name", StringType, nullable = true)
      )
    )
    val p = withLoadedPath(path, spark)
    spark.read
      .option("sep", "\t")
      .schema(schema)
      .csv(p)
      .withColumn("parents", lit(null).cast(ArrayType(StringType)))
      .as[OntologyTermBasic]
  }

  def loadTerms(path: String, spark: SparkSession): Dataset[OntologyTerm] = {
    import spark.implicits._
    val p = withLoadedPath(path, spark)
    spark.read.json(p).as[OntologyTerm]

  }

  def studiesExtraParams(
      path: String
  )(spark: SparkSession): Dataset[StudyExtraParams] = {

    val p = withLoadedPath(path, spark)
    spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load(p)
      .as[StudyExtraParams]
  }

  def loadCategory_ExistingDataTypes(
      path: String
  )(spark: SparkSession): Dataset[(String, Seq[String])] = {
    import spark.implicits._

    val p = withLoadedPath(path, spark)
    val rawLine = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load(p)
      .as[(String, String)]

    rawLine
      .map(t => (t._1, t._2.split(",").map(_.toLowerCase.trim)))
      .as[(String, Seq[String])]
  }

  def loadDuoLabel(path: String, spark: SparkSession): Dataset[DuoCode] = {
    import spark.implicits._
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable = false),
        StructField("shorthand", StringType),
        StructField("label", StringType, nullable = false),
        StructField("description", StringType)
      )
    )
    val p = withLoadedPath(path, spark)
    spark.read
      .option("sep", ",")
      .option("header", value = true)
      .schema(schema)
      .csv(p)
      .as[DuoCode]
  }

  private def withLoadedPath(path: String, spark: SparkSession) = {
    if (path.startsWith("http")) {
      spark.sparkContext.addFile(path)
      val filename = path.split("/").last
      SparkFiles.get(filename)
    } else path
  }

  def createStudies(
      studies: Seq[EStudy],
      studiesExtraParams: Dataset[StudyExtraParams]
  )(spark: SparkSession): Dataset[EStudy] = {
    import spark.implicits._
    val studiesDS = spark.createDataset(studies)

    val studies_with_extraParams = studiesDS.joinWith(
      studiesExtraParams,
      studiesDS.col("kf_id") === studiesExtraParams.col("kf_id"),
      "left_outer"
    )

    studies_with_extraParams.map {
      case (study: EStudy, param: StudyExtraParams) =>
        study.copy(
          code = param.code,
          domain = param.domain match {
            case Some(x) => x.split(",").toSeq
            case None    => Nil
          },
          program = param.program
        )
      case s => s._1
    }
  }

  def createDiagnosis(
      diagnoses: Seq[EDiagnosis],
      ontology: OntologiesDataSet,
      spark: SparkSession
  ): Dataset[EDiagnosis] = {

    import spark.implicits._
    val diagnosesDS = spark.createDataset(diagnoses)
    diagnosesDS
      .joinWith(
        ontology.mondoTerms,
        diagnosesDS("mondo_id_diagnosis") === ontology.mondoTerms("id"),
        "left_outer"
      )
      .as[(EDiagnosis, Option[OntologyTermBasic])]
      .joinWith(
        ontology.ncitTerms,
        $"_1.ncit_id_diagnosis" === ontology.ncitTerms("id"),
        "left_outer"
      )
      .map { case ((d, m), n) => (d, m, Option(n)) }
      .map {
        case (d, optMondoTerm, optNcitTerm)
            if optMondoTerm.isDefined && optNcitTerm.isDefined =>
          d.copy(
            diagnosis_text = optMondoTerm.map(_.name),
            mondo_id_diagnosis = optMondoTerm.map(_.id),
            ncit_id_diagnosis = formatTerm(optNcitTerm)
          )
        case (d, optMondoTerm, _) if optMondoTerm.isDefined =>
          d.copy(
            diagnosis_text = optMondoTerm.map(_.name),
            mondo_id_diagnosis = optMondoTerm.map(_.id),
            ncit_id_diagnosis = None
          )
        case (d, _, optNcitTerm) if optNcitTerm.isDefined =>
          d.copy(
            diagnosis_text = optNcitTerm.map(_.name),
            mondo_id_diagnosis = None,
            ncit_id_diagnosis = formatTerm(optNcitTerm)
          )
        case (d, _, _) =>
          d.copy(
            diagnosis_text = d.source_text_diagnosis,
            mondo_id_diagnosis = None,
            ncit_id_diagnosis = None
          )
      }
  }

  def formatTerm(term: Option[OntologyTermBasic]): Option[String] =
    term.map(t => s"${t.name} (${t.id})")
}
