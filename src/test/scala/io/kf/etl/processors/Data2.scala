package io.kf.etl.processors

import io.kf.etl.models.dataservice._

object Data2 {
  val bioSpecimen1: EBiospecimen = EBiospecimen(kfId = Some("BS_7TRMYRE1"),participantId = Some("PT_29ZEF7YN"), analyteType = Some("Not Reported"), ncitIdAnatomicalSite = Some("NCIT:C43234"))
  val bioSpecimen2: EBiospecimen = EBiospecimen(kfId = Some("BS_FBEYYF8M"), participantId = Some("PT_29ZEF7YN"), ncitIdAnatomicalSite = Some("NCIT:C43234"))
  val bioSpecimens: Seq[EBiospecimen] = Seq(bioSpecimen1, bioSpecimen2)

  val eBiospecimenGenomicFile1: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kfId = Some("eBiospecimenGenomicFile_id_1"), biospecimenId = Some("BS_7TRMYRE1"), genomicFileId = Some("GF_R43EF2XY"))
  val eBiospecimenGenomicFile: Seq[EBiospecimenGenomicFile] = Seq(eBiospecimenGenomicFile1)

  val participant1: EParticipant = EParticipant(kfId = Some("PT_29ZEF7YN"), ethnicity = Some("Hispanic or Latino"))
  val participants: Seq[EParticipant] = Seq(participant1)

  val genomicFile1: EGenomicFile = EGenomicFile(kfId = Some("GF_R43EF2XY"), dataType = Some("Aligned Read"), fileName = Some("C829.TARGET-30-PARJMX-01A-01D.3_gdc_realn.bam"))
  val genomicFiles: Seq[EGenomicFile] = Seq(genomicFile1)

  //----------------------

  val diagnosis1: EDiagnosis = EDiagnosis(kfId = Some("diagnosis_id_1"), participantId = Some("participant_id_1"))
  val diagnosis1_2: EDiagnosis = EDiagnosis(kfId = Some("diagnosis_id_1_2"), participantId = Some("participant_id_1"))
  val diagnosis1_3: EDiagnosis = EDiagnosis(kfId = Some("diagnosis_id_1_3"), participantId = Some("participant_id_1"))
  val diagnosis2: EDiagnosis = EDiagnosis(kfId = Some("diagnosis_id_2"), participantId = Some("participant_id_2"))
  val diagnosis: Seq[EDiagnosis] = Seq(diagnosis1, diagnosis1_2, diagnosis1_3, diagnosis2)

  val biospecimenDiagnosis1: EBiospecimenDiagnosis = EBiospecimenDiagnosis(kfId = Some("bd1"), diagnosisId = Some("diagnosis_id_1"), biospecimenId = Some("biospecimen_id_1"))
  val biospecimenDiagnosis2: EBiospecimenDiagnosis = EBiospecimenDiagnosis(kfId = Some("bd2"), diagnosisId = Some("diagnosis_id_1_2"), biospecimenId = Some("biospecimen_id_1_2"))
  val biospecimenDiagnosis3: EBiospecimenDiagnosis = EBiospecimenDiagnosis(kfId = Some("bd4"), diagnosisId = Some("diagnosis_id_2"), biospecimenId = Some("biospecimen_id_2"))
  val biospecimenDiagnosis4: EBiospecimenDiagnosis = EBiospecimenDiagnosis(kfId = Some("bd3"), diagnosisId = Some("diagnosis_id_1_3"), biospecimenId = Some("biospecimen_id_3"))
  val biospecimenDiagnosis: Seq[EBiospecimenDiagnosis] = Seq(biospecimenDiagnosis1, biospecimenDiagnosis2, biospecimenDiagnosis3, biospecimenDiagnosis4)

  val eSequencingExperiment1: ESequencingExperiment = ESequencingExperiment(kfId = Some("eSeqExp1"), genomicFiles = Seq("genomicFile7"), library_prep = Some("this_Prep1"), library_selection = Some("this_Selection1"))
  val eSequencingExperiment = Seq(eSequencingExperiment1)

  val eSequencingExperimentGenomicFile1: ESequencingExperimentGenomicFile = ESequencingExperimentGenomicFile(kfId = Some("eSeqExpGF1"), sequencingExperiment = Some("eSeqExp1"), genomicFile = Some("genomicFile7"))
  val eSequencingExperimentGenomicFile = Seq(eSequencingExperimentGenomicFile1)

}
