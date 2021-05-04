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


class FeatureCentricTransformerSpec extends FlatSpec with Matchers with WithSparkSession {
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


  "fileCentric" should "return the proper Sequence of FileCentric_ES" in {

    //Add biospecimens to participants beforehand to be passed to the service to be tested
    val participantId_Bios: Map[String, Seq[Biospecimen_ES]] = data.bioSpecimens.groupBy(_.participant_id.orNull).collect{ case(s, list) => (s, list.map(b => EntityConverter.EBiospecimenToBiospecimenES(b, Nil)))}

    val participants = data.participants
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

    val result = FeatureCentricTransformer.fileCentric(entityDataSet, participants)

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
        kf_id = Some("genomicFile7"),
        data_type = Some("Super Important type 7"),
        file_name = Some("File7"),
        participants = Nil,
        sequencing_experiments = Seq(
          SequencingExperiment_ES(
            kf_id = Some("eSeqExp1"),
            library_prep = Some("this_Prep1"),
            library_selection = Some("this_Selection1")
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

  "participantCentric" should "return the proper Sequence of ParticipantCentric_ES" in {

    //Convert all EBiospecimen to Biospecimen_ES
    val biospecimen_ES = data.bioSpecimens.map(b => EntityConverter.EBiospecimenToBiospecimenES(b))
    val phenotypes_ES = data.phenotypes

    //Enhance Participant_ES with all corresponding Biospeciment_ES
    val participant_ES =
      entityDataSet
        .participants
        .map(p => EntityConverter.EParticipantToParticipantES(p).copy(
          biospecimens = biospecimen_ES.filter(b => p.biospecimens.contains(b.kf_id.getOrElse(""))),
          available_data_types = p.kf_id.get match {
            case "participant_id_1" => Seq("Aligned Reads", "Radiology Images")
            case "participant_id_3" => Seq("Annotated Somatic Mutations", "Histology Images")
            case _ => Nil
          },
          phenotype = if (p.kf_id.get == "participant_id_4") phenotypes_ES else Nil)
        )

    val result = FeatureCentricTransformer.participantCentric(
      entityDataSet,
      participant_ES
    )

    result.collect() should contain theSameElementsAs Seq(
      ParticipantCentric_ES(
        kf_id = Some("participant_id_1"),
        available_data_types = Seq("Aligned Reads", "Radiology Images"),
        available_data_categories = Seq("Radiology", "Sequencing Reads"),
        biospecimens = Seq(
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_1"),
            genomic_files = Seq(
              GenomicFile_ES(
                kf_id = Some("genomicFile1"),
                data_type = Some("Aligned Reads"),
                file_name = Some("File1")
              ),
              GenomicFile_ES(
                kf_id = Some("genomicFile6"),
                data_type = Some("Super Important type 6"),
                file_name = Some("File6")
              )
            ),
            ncit_id_anatomical_site = Some("NCIT:unknown")
          ),
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_1_1"),
            genomic_files = Seq(
              GenomicFile_ES(
                kf_id = Some("genomicFile2"),
                data_type = Some("Super Important type 2"),
                file_name = Some("File2")
              )
            ),
            ncit_id_anatomical_site = Some("NCIT:unknown2")
          ),
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_1_2"),
            genomic_files = Seq(
              GenomicFile_ES(
                kf_id = Some("genomicFile3"),
                data_type = Some("Isoform Expression"),
                file_name = Some("File3")
              )
            ),
            ncit_id_anatomical_site = Some("NCIT:unknown")
          )
        ),
        files = Seq(
          GenomicFile_ES(
            kf_id = Some("genomicFile1"),
            data_type = Some("Aligned Reads"),
            file_name = Some("File1")
          ),
          GenomicFile_ES(
            kf_id = Some("genomicFile6"),
            data_type = Some("Super Important type 6"),
            file_name = Some("File6")
          ),
          GenomicFile_ES(
            kf_id = Some("genomicFile2"),
            data_type = Some("Super Important type 2"),
            file_name = Some("File2")
          ),
          GenomicFile_ES(
            kf_id = Some("genomicFile3"),
            data_type = Some("Isoform Expression"),
            file_name = Some("File3")
          )
        )
      ),
      ParticipantCentric_ES(
        kf_id = Some("participant_id_2"),
        biospecimens = Seq(
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_2"),
            genomic_files = Seq(
              GenomicFile_ES(
                kf_id = Some("genomicFile4"),
                data_type = Some("Super Important type 4"),
                file_name = Some("File4")
              )
            ),
            ncit_id_anatomical_site = Some("NCIT:C12438"),
            ncit_id_tissue_type = Some("NCIT:C14165")
          )
        ),
        files = Seq(
          GenomicFile_ES(
            kf_id = Some("genomicFile4"),
            data_type = Some("Super Important type 4"),
            file_name = Some("File4")
          )
        ),
        race = Some("klingon")
      ),
      ParticipantCentric_ES(
        kf_id = Some("participant_id_3"),
        available_data_types = Seq("Annotated Somatic Mutations", "Histology Images"),
        available_data_categories = Seq("Simple Nucleotide Variation", "Pathology"),
        biospecimens = Seq(
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_3"),
            genomic_files = Seq(
              GenomicFile_ES(
                kf_id = Some("genomicFile1"),
                data_type = Some("Aligned Reads"),
                file_name = Some("File1")
              ),
              GenomicFile_ES(
                kf_id = Some("genomicFile5"),
                data_type = Some("Super Important type 5"),
                file_name = Some("File5")
              )
            )
          )
        ),
        files = Seq(
          GenomicFile_ES(
            kf_id = Some("genomicFile1"),
            data_type = Some("Aligned Reads"),
            file_name = Some("File1")
          ),
          GenomicFile_ES(
            kf_id = Some("genomicFile5"),
            data_type = Some("Super Important type 5"),
            file_name = Some("File5")
          )
        )
      ),
      ParticipantCentric_ES(
        kf_id = Some("participant_id_4"),
        phenotype = Seq(
          Phenotype_ES(
            age_at_event_days = Some(15),
            hpo_phenotype_observed = Some("Osteolytic defect of thumb phalanx (HP:0009654)"),
            hpo_phenotype_observed_text = Some("Osteolytic defect of thumb phalanx (HP:0009654)"),
            observed = Some(true)
          ),
          Phenotype_ES(
            age_at_event_days = Some(18),
            hpo_phenotype_observed = Some("Abnormal upper limb bone morphology (HP:0045081)"),
            hpo_phenotype_observed_text = Some("Abnormal upper limb bone morphology (HP:0045081)"),
            observed = Some(true)
          )
        )
      ),
      ParticipantCentric_ES(
        kf_id = Some("participant_id_5"),
        biospecimens = Seq(
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_6"),
            genomic_files = Seq(
              GenomicFile_ES(
                kf_id = Some("genomicFile8"),
                data_type = Some("Super Important type 8"),
                file_name = Some("File8")
              )
            ),
            duo_code = Seq("duo_id1")
          ),
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_5"),
            genomic_files = Nil
          )
        ),
        files = Seq(
          GenomicFile_ES(
            kf_id = Some("genomicFile8"),
            data_type = Some("Super Important type 8"),
            file_name = Some("File8")
          )
        )
      )
    )
  }

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
      EStudy(kf_id = Some("study"), domain = Seq("Cancer", "Birth Defect"), short_name = Some("study short name")),
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