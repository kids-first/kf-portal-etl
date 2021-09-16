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


class ParticipantCentricTransformerSpec extends FlatSpec with Matchers with WithSparkSession {
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
}
