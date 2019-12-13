package io.kf.etl.processors.participantcentric

import com.typesafe.config.Config
import io.kf.etl.models.dataservice._
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}


class ParticipantCentricProcessorSpec extends FlatSpec with Matchers with WithSparkSession {
  import spark.implicits._

  implicit var config: Config = _

  val bioSpecimen1 = EBiospecimen(kfId = Some("biospecimen_id_1"), participantId = Some("participant_id_1"), ncitIdAnatomicalSite = Some("NCIT:unknown"))
  val bioSpecimen1_1 = EBiospecimen(kfId = Some("biospecimen_id_1_1"), participantId = Some("participant_id_1"), ncitIdAnatomicalSite = Some("NCIT:unknown2"))
  val bioSpecimen1_2 = EBiospecimen(kfId = Some("biospecimen_id_1_2"), participantId = Some("participant_id_1"), ncitIdAnatomicalSite = Some("NCIT:unknown"))
  val bioSpecimen2 = EBiospecimen(kfId = Some("biospecimen_id_2"), participantId = Some("participant_id_2"), ncitIdAnatomicalSite = Some("NCIT:C12438"), ncitIdTissueType = Some("NCIT:C14165"))
  val bioSpecimen3 = EBiospecimen(kfId = Some("biospecimen_id_3"), participantId = Some("participant_id_3"))
  val bioSpecimen4 = EBiospecimen(kfId = Some("biospecimen_id_4"))
  val bioSpecimens: Seq[EBiospecimen] = Seq(bioSpecimen1, bioSpecimen2, bioSpecimen3, bioSpecimen4)

  val participant1 = EParticipant(kfId = Some("participant_id_1"), biospecimens = Seq(bioSpecimen1.kfId.orNull, bioSpecimen1_1.kfId.orNull, bioSpecimen1_2.kfId.orNull))
  val participant2 = EParticipant(kfId = Some("participant_id_2"), biospecimens = Seq(bioSpecimen2.kfId.orNull))
  val participant3 = EParticipant(kfId = Some("participant_id_3"), biospecimens = Seq(bioSpecimen3.kfId.orNull))
  val participant4 = EParticipant(kfId = Some("participant_id_3"), biospecimens = Seq(bioSpecimen4.kfId.orNull))
  val participants: Seq[EParticipant] = Seq(participant1, participant2, participant3, participant4)

  val diagnosis1 = EDiagnosis(kfId = Some("diagnosis_id_1"), participantId = Some("participant_id_1"))
  val diagnosis1_2 = EDiagnosis(kfId = Some("diagnosis_id_1_2"), participantId = Some("participant_id_1"))
  val diagnosis1_3 = EDiagnosis(kfId = Some("diagnosis_id_1_3"), participantId = Some("participant_id_1"))
  val diagnosis2 = EDiagnosis(kfId = Some("diagnosis_id_2"), participantId = Some("participant_id_2"))
  val diagnosis: Seq[EDiagnosis] = Seq(diagnosis1, diagnosis1_2, diagnosis1_3, diagnosis2)

  val genomicFile1 = EGenomicFile(kfId = Some("genomicFile1"), dataType = Some("Super Important type 1"))
  val genomicFile2 = EGenomicFile(kfId = Some("genomicFile2"), dataType = Some("Super Important type 2"))
  val genomicFile3 = EGenomicFile(kfId = Some("genomicFile3"), dataType = Some("Super Important type 3"))
  val genomicFile4 = EGenomicFile(kfId = Some("genomicFile4"), dataType = Some("Super Important type 4"))
  val genomicFile5 = EGenomicFile(kfId = Some("genomicFile5"), dataType = Some("Super Important type 5"))
  val genomicFile6 = EGenomicFile(kfId = Some("genomicFile6"), dataType = Some("Super Important type 6"))
  val genomicFile7 = EGenomicFile(kfId = Some("genomicFile7"), dataType = Some("Super Important type 6"))
  val genomicFiles: Seq[EGenomicFile] = Seq(genomicFile1, genomicFile2, genomicFile3, genomicFile4, genomicFile5, genomicFile6)

  val eBiospecimenGenomicFile1: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kfId = Some("eBiospecimenGenomicFile_id_1"), biospecimenId = Some("biospecimen_id_1"), genomicFileId = Some("genomicFile1"))
  val eBiospecimenGenomicFile2: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kfId = Some("eBiospecimenGenomicFile_id_2"), biospecimenId = Some("biospecimen_id_1_1"), genomicFileId = Some("genomicFile2"))
  val eBiospecimenGenomicFile3: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kfId = Some("eBiospecimenGenomicFile_id_3"), biospecimenId = Some("biospecimen_id_1_2"), genomicFileId = Some("genomicFile3"))
  val eBiospecimenGenomicFile4 = EBiospecimenGenomicFile(kfId = Some("eBiospecimenGenomicFile_id_4"), biospecimenId = Some("biospecimen_id_2"), genomicFileId = Some("genomicFile4"))
  val eBiospecimenGenomicFile5 = EBiospecimenGenomicFile(kfId = Some("eBiospecimenGenomicFile_id_5"), biospecimenId = Some("biospecimen_id_3"), genomicFileId = Some("genomicFile5"))
  val eBiospecimenGenomicFile6 = EBiospecimenGenomicFile(kfId = Some("eBiospecimenGenomicFile_id_6"), biospecimenId = Some("biospecimen_id_4"), genomicFileId = Some("genomicFile6"))
  val eBiospecimenGenomicFile: Seq[EBiospecimenGenomicFile] = Seq(eBiospecimenGenomicFile1, eBiospecimenGenomicFile2, eBiospecimenGenomicFile3,eBiospecimenGenomicFile4, eBiospecimenGenomicFile5, eBiospecimenGenomicFile6)

  val biospecimenDiagnosis1 = EBiospecimenDiagnosis(kfId = Some("bd1"), diagnosisId = Some("diagnosis_id_1"), biospecimenId = Some("biospecimen_id_1"))
  val biospecimenDiagnosis2 = EBiospecimenDiagnosis(kfId = Some("bd2"), diagnosisId = Some("diagnosis_id_1_2"), biospecimenId = Some("biospecimen_id_1_2"))
  val biospecimenDiagnosis3 = EBiospecimenDiagnosis(kfId = Some("bd4"), diagnosisId = Some("diagnosis_id_2"), biospecimenId = Some("biospecimen_id_2"))
  val biospecimenDiagnosis4 = EBiospecimenDiagnosis(kfId = Some("bd3"), diagnosisId = Some("diagnosis_id_1_3"), biospecimenId = Some("biospecimen_id_3"))
  val biospecimenDiagnosis: Seq[EBiospecimenDiagnosis] = Seq(biospecimenDiagnosis1, biospecimenDiagnosis2, biospecimenDiagnosis3, biospecimenDiagnosis4)

  val eSequencingExperiment1: ESequencingExperiment = ESequencingExperiment(kfId = Some("eSeqExp1"), genomicFiles = Seq("genomicFile7"))
  val eSequencingExperiment = Seq(eSequencingExperiment1)

  val eSequencingExperimentGenomicFile1 = ESequencingExperimentGenomicFile(kfId = Some("eSeqExpGF1"), sequencingExperiment = Some("eSeqExp1"), genomicFile = Some("genomicFile7"))
  val eSequencingExperimentGenomicFile = Seq(eSequencingExperimentGenomicFile1)

  val entityDataSet: EntityDataSet = buildEntityDataSet(
    participants = participants,
    biospecimens = bioSpecimens,
    diagnoses = diagnosis,
    genomicFiles = genomicFiles,
    biospecimenGenomicFiles = eBiospecimenGenomicFile,
    biospecimenDiagnoses = biospecimenDiagnosis,
    sequencingExperiments = eSequencingExperiment,
    sequencingExperimentGenomicFiles = eSequencingExperimentGenomicFile
  )

  "Test" should "load ontological terms from comnpressed TSV file" in {

    ParticipantCentricProcessor.apply(entityDataSet, participants.map(EntityConverter.EParticipantToParticipantES).toDS())

    1 shouldBe 2

  }
}

//(
//affected_status: scala.Option[Boolean] = None,
//alias_group: scala.Option[String] = None,
//
//available_data_types: _root_.scala.collection.Seq[String] = _root_.scala.collection.Seq.empty,
//
//biospecimens: _root_.scala.collection.Seq[Biospecimen_ES] = _root_.scala.collection.Seq.empty,
//diagnoses: _root_.scala.collection.Seq[Diagnosis_ES] = _root_.scala.collection.Seq.empty,
//diagnosis_category: scala.Option[String] = None,
//ethnicity: scala.Option[String] = None,
//external_id: scala.Option[String] = None,
//family: scala.Option[Family_ES] = None,
//family_id: scala.Option[String] = None,
//gender: scala.Option[String] = None,
//is_proband: scala.Option[Boolean] = None,
//kf_id: scala.Option[String] = None,
//outcome: scala.Option[Outcome_ES] = None,
//phenotype: _root_.scala.collection.Seq[Phenotype_ES] = _root_.scala.collection.Seq.empty,
//race: scala.Option[String] = None,
//study: scala.Option[Study_ES] = None
//)


