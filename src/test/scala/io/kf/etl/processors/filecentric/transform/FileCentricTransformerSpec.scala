package io.kf.etl.processors.filecentric.transform

import com.typesafe.config.Config
import io.kf.etl.models.es.{Biospecimen_ES, FileCentric_ES, GenomicFile_ES, Participant_ES, SequencingExperiment_ES}
import io.kf.etl.processors.Data
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import io.kf.etl.processors.featurecentric.transform.FeatureCentricTransformer
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}


class FileCentricTransformerSpec extends FlatSpec with Matchers with WithSparkSession {
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

  val test = Seq(
    FileCentric_ES(
      kf_id = Some("genomicFile1"),
      data_type = Some("Super Important type 1"),
      file_name = Some("File1"),
      participants = Seq(
        Participant_ES(
          kf_id = Some("participant_id_3")
        ),
        Participant_ES(
          kf_id = Some("participant_id_1")
        )
      )
    ),
    FileCentric_ES(
      kf_id = Some("genomicFile2"),
      data_type = Some("Super Important type 2"),
      file_name = Some("File2"),
      participants = Seq(
        Participant_ES(
          kf_id = Some("participant_id_1")
        )
      )
    ),
    FileCentric_ES(
      kf_id = Some("genomicFile3"),
      data_type = Some("Super Important type 3"),
      file_name = Some("File3"),
      participants = Seq(
        Participant_ES(
          kf_id = Some("participant_id_1")
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
          kf_id = Some("participant_id_3")
        )
      )
    ),
    FileCentric_ES(
      kf_id = Some("genomicFile6"),
      data_type = Some("Super Important type 6"),
      file_name = Some("File6"),
      participants = Seq(
        Participant_ES(
          kf_id = Some("participant_id_1")
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
          kf_id = Some("eSeqExp1")
        )
      )
    )
  )

  "apply" should "return the proper Sequence of ParticipantCombined_ES" in {

    val result = FeatureCentricTransformer.file(entityDataSet, Data.participants.map(EntityConverter.EParticipantToParticipantES).toDS())

    //FIXME
    // theSameElementsAs has arguably a bug. Ordering is important for this test to pass for Sets < 5.
    // See https://github.com/scala-exercises/scala-exercises/issues/18
    result.collect() should contain theSameElementsAs Seq(
      FileCentric_ES(
        kf_id = Some("genomicFile1"),
        data_type = Some("Super Important type 1"),
        file_name = Some("File1"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_3")
          ),
          Participant_ES(
            kf_id = Some("participant_id_1")
          )
        )
      ),
      FileCentric_ES(
        kf_id = Some("genomicFile2"),
        data_type = Some("Super Important type 2"),
        file_name = Some("File2"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_1")
          )
        )
      ),
      FileCentric_ES(
        kf_id = Some("genomicFile3"),
        data_type = Some("Super Important type 3"),
        file_name = Some("File3"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_1")
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
            kf_id = Some("participant_id_3")
          )
        )
      ),
      FileCentric_ES(
        kf_id = Some("genomicFile6"),
        data_type = Some("Super Important type 6"),
        file_name = Some("File6"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_1")
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
            kf_id = Some("eSeqExp1")
          )
        )
      )
    )
  }
}