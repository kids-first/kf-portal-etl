package io.kf.etl.processors.featurecentric.transform

import com.typesafe.config.Config
import io.kf.etl.models.es._
import io.kf.etl.processors.Data
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import io.kf.etl.processors.download.transform.DownloadTransformer
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}


class FileCentricTransformerSpec extends FlatSpec with Matchers with WithSparkSession {
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

  //Add biospecimens to participants beforehand to be passed to the service to be tested
  val participantId_Bios: Map[String, Seq[Biospecimen_ES]] =
    data.bioSpecimens
      .groupBy(_.participant_id.orNull)
      .collect{ case(s, list) => (s, list.map(b => EntityConverter.EBiospecimenToBiospecimenES(b, Nil)))}

  val participants: Dataset[Participant_ES] = data.participants
    .map(EntityConverter.EParticipantToParticipantES)
    .map(p => p.copy(
      biospecimens = if(participantId_Bios.contains(p.kf_id.get)) {participantId_Bios(p.kf_id.orNull)} else Nil,
      study = if(p.kf_id.contains("participant_id_1"))
        Some(Study_ES(
          kf_id = Some("SD_46SK55A3"),
          short_name = Some("shart name"),
          program = Some("Kids First"),
          domain = Seq("Birth Defect"),
          code = Some("KF-CDH")
        )) else None
    ))
    .toDS()

  val result: Dataset[FileCentric_ES] = FeatureCentricTransformer.fileCentric(entityDataSet, participants)


  "fileCentric" should "return the proper Sequence of FileCentric_ES" in {

    result.collect() should contain theSameElementsAs Seq(
      FileCentric_ES(
        kf_id = Some("genomicFile1"),
        data_type = Some("Aligned Reads"),
        data_category = Some("Sequencing Reads"),
        file_name = Some("File1"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_3"),
            biospecimens = Seq(
              Biospecimen_ES(
                kf_id = Some("biospecimen_id_3")
              )
            )
          ),
          Participant_ES(
            kf_id = Some("participant_id_1"),
            biospecimens = Seq(
              Biospecimen_ES(
                kf_id = Some("biospecimen_id_1"),
                ncit_id_anatomical_site = Some("NCIT:unknown")
              )
            ),
            study = Some(Study_ES(
              kf_id = Some("SD_46SK55A3"),
              short_name = Some("shart name"),
              program = Some("Kids First"),
              domain = Seq("Birth Defect"),
              code = Some("KF-CDH")
            ))
          )
        )
      ),
      FileCentric_ES(
        kf_id = Some("genomicFile2"),
        data_type = Some("Super Important type 2"),
        file_name = Some("File2"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_1"),
            biospecimens = Seq(
              Biospecimen_ES(
                kf_id = Some("biospecimen_id_1_1"),
                ncit_id_anatomical_site = Some("NCIT:unknown2")
              )
            ),
            study = Some(Study_ES(
              kf_id = Some("SD_46SK55A3"),
              short_name = Some("shart name"),
              program = Some("Kids First"),
              domain = Seq("Birth Defect"),
              code = Some("KF-CDH")
            ))
          )
        )
      ),
      FileCentric_ES(
        kf_id = Some("genomicFile3"),
        data_type = Some("Isoform Expression"),
        data_category = Some("Transcriptome Profiling"),
        file_name = Some("File3"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_1"),
            biospecimens = Seq(
              Biospecimen_ES(
                kf_id = Some("biospecimen_id_1_2"),
                ncit_id_anatomical_site = Some("NCIT:unknown")
              )
            ),
            study = Some(Study_ES(
              kf_id = Some("SD_46SK55A3"),
              short_name = Some("shart name"),
              program = Some("Kids First"),
              domain = Seq("Birth Defect"),
              code = Some("KF-CDH")
            ))
          )
        )
      ),
      FileCentric_ES(
        kf_id = Some("genomicFile4"),
        data_type = Some("Super Important type 4"),
        file_name = Some("File4"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_2"),
            biospecimens = Seq(
              Biospecimen_ES(
                kf_id = Some("biospecimen_id_2"),
                ncit_id_anatomical_site = Some("NCIT:C12438"),
                ncit_id_tissue_type = Some("NCIT:C14165")
              )
            ),
            race = Some("klingon")
          )
        )
      ),
      FileCentric_ES(
        kf_id = Some("genomicFile5"),
        data_type = Some("Super Important type 5"),
        file_name = Some("File5"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_3"),
            biospecimens = Seq(
              Biospecimen_ES(
                kf_id = Some("biospecimen_id_3")
              )
            )
          )
        )
      ),
      FileCentric_ES(
        kf_id = Some("genomicFile6"),
        data_type = Some("Super Important type 6"),
        file_name = Some("File6"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_1"),
            biospecimens = Seq(
              Biospecimen_ES(
                kf_id = Some("biospecimen_id_1"),
                ncit_id_anatomical_site = Some("NCIT:unknown")
              )
            ),
            study = Some(Study_ES(
              kf_id = Some("SD_46SK55A3"),
              short_name = Some("shart name"),
              program = Some("Kids First"),
              domain = Seq("Birth Defect"),
              code = Some("KF-CDH")
            ))
          )
        )
      ),
      FileCentric_ES(
        kf_id = Some("genomicFile8"),
        data_type = Some("Super Important type 8"),
        file_name = Some("File8"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_5"),
            biospecimens = Seq(
              Biospecimen_ES(
                kf_id = Some("biospecimen_id_6"),
                duo_code = Seq("duo_id1")
              )
            )
          )
        )
      )
    )
  }

  it should "filter out files with no participants and remove participant form files with no biospecimen" in {

    val filesIds = result.select("kf_id").as[String].collectAsList()

    // genomicFile7 does not have any participants
    filesIds shouldNot contain ("genomicFile7")

  }
}
