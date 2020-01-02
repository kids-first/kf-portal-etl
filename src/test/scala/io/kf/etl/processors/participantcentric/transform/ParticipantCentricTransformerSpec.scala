package io.kf.etl.processors.participantcentric.transform

import com.typesafe.config.Config
import io.kf.etl.models.es.{Biospecimen_ES, GenomicFile_ES, ParticipantCombined_ES}
import io.kf.etl.processors.Data
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import io.kf.etl.processors.featurecentric.transform.FeatureCentricTransformer
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}


class ParticipantCentricTransformerSpec extends FlatSpec with Matchers with WithSparkSession {
  import spark.implicits._

  implicit var config: Config = _

  val entityDataSet: EntityDataSet = buildEntityDataSet(
    participants = Data.participants,
    biospecimens = Data.bioSpecimens,
    diagnoses = Data.diagnosis,
    genomicFiles = Data.genomicFiles,
    biospecimenGenomicFiles = Data.eBiospecimenGenomicFile,
    biospecimenDiagnoses = Data.biospecimenDiagnosis,
    sequencingExperiments = Data.eSequencingExperiment,
    sequencingExperimentGenomicFiles = Data.eSequencingExperimentGenomicFile
  )

  "apply" should "return the proper Sequence of ParticipantCombined_ES" in {

    val result = FeatureCentricTransformer.participant(entityDataSet, Data.participants.map(EntityConverter.EParticipantToParticipantES).toDS())

//    result.show(truncate = false)

    result.collect() should contain theSameElementsAs Seq(
      ParticipantCombined_ES(
        kf_id = Some("participant_id_1"),
        biospecimens = Seq(
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_1"),
            genomic_files = Seq(
              GenomicFile_ES(
                kf_id = Some("genomicFile1"),
                data_type = Some("Super Important type 1"),
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
                data_type = Some("Super Important type 3"),
                file_name = Some("File3")
              )
            ),
            ncit_id_anatomical_site = Some("NCIT:unknown")
          )
        ),
        files = Seq(
          GenomicFile_ES(
            kf_id = Some("genomicFile1"),
            data_type = Some("Super Important type 1"),
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
            data_type = Some("Super Important type 3"),
            file_name = Some("File3")
          )
        )
      ),
      ParticipantCombined_ES(
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
      ParticipantCombined_ES(
        kf_id = Some("participant_id_3"),
        biospecimens = Seq(
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_3"),
            genomic_files = Seq(
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
            kf_id = Some("genomicFile5"),
            data_type = Some("Super Important type 5"),
            file_name = Some("File5")
          )
        )
      ),
      ParticipantCombined_ES(
        kf_id = Some("participant_id_4")
      )
    )
  }
}



































