package io.kf.etl.processors.featurecentric.transform

import com.typesafe.config.Config
import io.kf.etl.models.dataservice.EStudy
import io.kf.etl.models.es._
import io.kf.etl.processors.Data
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import io.kf.etl.processors.download.transform.DownloadTransformer
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}


class StudyCentricTransformerSpec extends FlatSpec with Matchers with WithSparkSession {
  import spark.implicits._

  implicit var config: Config = _

  val data: Data.type = Data

  val mapOfDataCategory_ExistingTypes: Option[Dataset[(String, Seq[String])]] =
    Some(DownloadTransformer.loadCategory_ExistingDataTypes("./src/test/resources/data_category_existing_data.tsv")(spark))

  val entityDataSet: EntityDataSet = buildEntityDataSet(
    participants = data.participants,
    biospecimens = data.bioSpecimens,
    diagnoses = data.diagnosis,
    genomicFiles = data.genomicFiles,
    biospecimenGenomicFiles = data.eBiospecimenGenomicFile,
    biospecimenDiagnoses = data.biospecimenDiagnosis,
    sequencingExperiments = data.eSequencingExperiment,
    sequencingExperimentGenomicFiles = data.eSequencingExperimentGenomicFile,
    duoCodes = Some(data.duoCodes.toDS()),
    mapOfDataCategory_ExistingTypes = mapOfDataCategory_ExistingTypes
  )
  val available_data_types: Seq[(String, Seq[String])] = data.available_data_types

  "studyCentric" should "return the proper Sequence of StudyCentric_ES" in {

    val participants_ds = Seq(
      ParticipantCentric_ES (
        kf_id = Some("participant1"),
        family_id = Some("fam1"),
        available_data_types = Seq("Pathology Reports", "Radiology Reports"),
        is_proband = Some(true)
      ),
      ParticipantCentric_ES (
        kf_id = Some("participant2"),
        family_id = Some("fam1"),
        available_data_types = Seq("Aligned Reads", "Unaligned Reads", "Radiology Images"),
        is_proband = Some(true)
      ),
      ParticipantCentric_ES (
        kf_id = Some("participant3"),
        family_id = Some("fam2"),
        available_data_types = Seq("Other", "Gene Expression"),
        is_proband = Some(false)
      ),
      ParticipantCentric_ES (
        kf_id = Some("participant4"),
        family_id = None,
        available_data_types = Seq("Histology Images", "Annotated Somatic Mutations", "Toto"),
        is_proband = Some(true)
      ),
      ParticipantCentric_ES (
        kf_id = Some("participant5"),
        family_id = None,
        is_proband = Some(true)
      )
    ).toDS()

    val files_ds = Seq(
      FileCentric_ES (
        kf_id = Some("file1"),
        sequencing_experiments = Seq(SequencingExperiment_ES(experiment_strategy=Some("one")))
      ),
      FileCentric_ES (
        kf_id = Some("file2"),
        sequencing_experiments = Seq(SequencingExperiment_ES(experiment_strategy=Some("two")))
      ),
      FileCentric_ES (
        kf_id = Some("file3"),
        sequencing_experiments = Seq(
          SequencingExperiment_ES(experiment_strategy=Some("three")),
          SequencingExperiment_ES(experiment_strategy=Some("one"))
        )
      )
    ).toDS()

    val study = "study"

    val studies = Seq(
      EStudy(kf_id = Some("study"), domain = Some("CANCERANDBIRTHDEFECT"), short_name = Some("study short name")),
      EStudy(kf_id = None),
      EStudy(kf_id = Some("other_study"))
    )

    val entityDataSet = buildEntityDataSet(
      studies = studies,
      mapOfDataCategory_ExistingTypes = mapOfDataCategory_ExistingTypes
    )

    val result = FeatureCentricTransformer.studyCentric(entityDataSet, study, participants_ds, files_ds).collect()

    val expectedResult = Seq(
      StudyCentric_ES(
        kf_id = Some("study"),
        name = Some("study short name"),
        search = Seq("study short name"),
        participant_count = Some(5),
        domain = Seq("Cancer", "Birth Defect"),
        file_count = Some(3),
        family_count = Some(2),
        family_data = Some(true),
        experimental_strategy = Seq("one", "two", "three"),
        data_categories =
          Seq(
            "Sequencing Reads", "Pathology", "Other", "Simple Nucleotide Variation", "Transcriptome Profiling", "Radiology"
          ),
        data_category_count = Seq(
          DataCategoryWCount_ES(
            data_category = "Sequencing Reads",
            count = 1
          ),
          DataCategoryWCount_ES(
            data_category = "Pathology",
            count = 2
          ),
          DataCategoryWCount_ES(
            data_category = "Other",
            count = 1
          ),
          DataCategoryWCount_ES(
            data_category = "Simple Nucleotide Variation",
            count = 1
          ),
          DataCategoryWCount_ES(
            data_category = "Transcriptome Profiling",
            count = 1
          ),
          DataCategoryWCount_ES(
            data_category = "Radiology",
            count = 2
          )

        )
      )
    )

    result should contain theSameElementsAs expectedResult

  }
}
