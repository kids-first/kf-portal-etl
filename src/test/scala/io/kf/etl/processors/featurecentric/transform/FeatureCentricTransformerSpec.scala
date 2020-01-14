package io.kf.etl.processors.featurecentric.transform

import com.typesafe.config.Config
import io.kf.etl.models.es.{Biospecimen_ES, FileCentric_ES, GenomicFile_ES, ParticipantCentric_ES, Participant_ES, SequencingExperiment_ES}
import io.kf.etl.processors.Data
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}


class FeatureCentricTransformerSpec extends FlatSpec with Matchers with WithSparkSession {
  import spark.implicits._

  implicit var config: Config = _

  val data: Data.type = Data
  val entityDataSet: EntityDataSet = buildEntityDataSet(
    participants = data.participants,
    biospecimens = data.bioSpecimens,
    diagnoses = data.diagnosis,
    genomicFiles = data.genomicFiles,
    biospecimenGenomicFiles = data.eBiospecimenGenomicFile,
    biospecimenDiagnoses = data.biospecimenDiagnosis,
    sequencingExperiments = data.eSequencingExperiment,
    sequencingExperimentGenomicFiles = data.eSequencingExperimentGenomicFile
  )

  "fileCentric" should "return the proper Sequence of FileCentric_ES" in {

    val duoCodes = data.duoCodes.toDS()


    //Add biospecimens to participants beforehand to be passed to the service to be tested
    val participantId_Bios: Map[String, Seq[Biospecimen_ES]] = data.bioSpecimens.groupBy(_.participantId.orNull).collect{ case(s, list) => (s, list.map(b => EntityConverter.EBiospecimenToBiospecimenCombinedES(b, Nil, Option(duoCodes))))}

    val participants = data.participants
      .map(EntityConverter.EParticipantToParticipantES)
      .map(p => p.copy(biospecimens = if(participantId_Bios.contains(p.kf_id.get)) {participantId_Bios(p.kf_id.orNull)} else Nil))
      .toDS()

    val result = FeatureCentricTransformer.fileCentricOld(entityDataSet, participants)

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
            kf_id = Some("participant_id_1"),
            biospecimens = Seq(
              Biospecimen_ES(
                kf_id = Some("biospecimen_id_1"),
                ncit_id_anatomical_site = Some("NCIT:unknown")
              )
            )
          ),
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
            )
          )
        )
      ),
      FileCentric_ES(
        kf_id = Some("genomicFile3"),
        data_type = Some("Super Important type 3"),
        file_name = Some("File3"),
        participants = Seq(
          Participant_ES(
            kf_id = Some("participant_id_1"),
            biospecimens = Seq(
              Biospecimen_ES(
                kf_id = Some("biospecimen_id_1_2"),
                ncit_id_anatomical_site = Some("NCIT:unknown")
              )
            )
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
            )
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
                kf_id = Some("biospecimen_id_6")
              )
            )
          )
        )
      )
    )
  }

  "participantCentric" should "return the proper Sequence of ParticipantCentric_ES" in {

    val result = FeatureCentricTransformer.participantCentric(entityDataSet, entityDataSet.participants.map(EntityConverter.EParticipantToParticipantES))

//    result.show(truncate = false)

    //FIXME
    // theSameElementsAs has arguably a bug. Ordering is important for this test to pass for Sets < 5.
    // See https://github.com/scala-exercises/scala-exercises/issues/18
    result.collect() should contain theSameElementsAs Seq(
      ParticipantCentric_ES(
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
        biospecimens = Seq(
          Biospecimen_ES(
            kf_id = Some("biospecimen_id_3"),
            genomic_files = Seq(
              GenomicFile_ES(
                kf_id = Some("genomicFile1"),
                data_type = Some("Super Important type 1"),
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
            data_type = Some("Super Important type 1"),
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
        kf_id = Some("participant_id_4")
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
            )
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