package io.kf.etl.processors

import io.kf.etl.models.dataservice._
import io.kf.etl.models.duocode.DuoCode
import io.kf.etl.models.es.Phenotype_ES

object Data {
  val bioSpecimen1: EBiospecimen = EBiospecimen(kf_id = Some("biospecimen_id_1"), participant_id = Some("participant_id_1"), ncit_id_anatomical_site = Some("NCIT:unknown"))
  val bioSpecimen1_1: EBiospecimen = EBiospecimen(kf_id = Some("biospecimen_id_1_1"), participant_id = Some("participant_id_1"), ncit_id_anatomical_site = Some("NCIT:unknown2"))
  val bioSpecimen1_2: EBiospecimen = EBiospecimen(kf_id = Some("biospecimen_id_1_2"), participant_id = Some("participant_id_1"), ncit_id_anatomical_site = Some("NCIT:unknown"))
  val bioSpecimen2: EBiospecimen = EBiospecimen(kf_id = Some("biospecimen_id_2"), participant_id = Some("participant_id_2"), ncit_id_anatomical_site = Some("NCIT:C12438"), ncit_id_tissue_type = Some("NCIT:C14165"))
  val bioSpecimen3: EBiospecimen = EBiospecimen(kf_id = Some("biospecimen_id_3"), participant_id = Some("participant_id_3"))
  val bioSpecimen4: EBiospecimen = EBiospecimen(kf_id = Some("biospecimen_id_4")) //Does not have a participant
  val bioSpecimen5: EBiospecimen = EBiospecimen(kf_id = Some("biospecimen_id_5"), participant_id = Some("participant_id_5"))
  val bioSpecimen6: EBiospecimen = EBiospecimen(kf_id = Some("biospecimen_id_6"), participant_id = Some("participant_id_5"), duo_ids = Seq("duo_id1"))
  val bioSpecimens: Seq[EBiospecimen] = Seq(bioSpecimen1, bioSpecimen1_1, bioSpecimen1_2, bioSpecimen2, bioSpecimen3, bioSpecimen4, bioSpecimen5, bioSpecimen6)

  val participant1: EParticipant = EParticipant(kf_id = Some("participant_id_1"), biospecimens = Seq("biospecimen_id_1", "biospecimen_id_1_1", "biospecimen_id_1_2"))
  val participant2: EParticipant = EParticipant(kf_id = Some("participant_id_2"), biospecimens = Seq("biospecimen_id_2"), diagnoses = Seq("diagnosis_id_2"), race = Some("klingon"))
  val participant3: EParticipant = EParticipant(kf_id = Some("participant_id_3"), biospecimens = Seq("biospecimen_id_3"))
  val participant4: EParticipant = EParticipant(kf_id = Some("participant_id_4"))
  val participant5: EParticipant = EParticipant(kf_id = Some("participant_id_5"), biospecimens = Seq("biospecimen_id_5", "biospecimen_id_6"))
  val participants: Seq[EParticipant] = Seq(participant1, participant2, participant3, participant4, participant5)

  val diagnosis1: EDiagnosis = EDiagnosis(kf_id = Some("diagnosis_id_1"), participant_id = Some("participant_id_1"))
  val diagnosis1_2: EDiagnosis = EDiagnosis(kf_id = Some("diagnosis_id_1_2"), participant_id = Some("participant_id_1"))
  val diagnosis1_3: EDiagnosis = EDiagnosis(kf_id = Some("diagnosis_id_1_3"), participant_id = Some("participant_id_1"))
  val diagnosis2: EDiagnosis = EDiagnosis(kf_id = Some("diagnosis_id_2"), participant_id = Some("participant_id_2"))
  val diagnosis: Seq[EDiagnosis] = Seq(diagnosis1, diagnosis1_2, diagnosis1_3, diagnosis2)

  val genomicFile1: EGenomicFile = EGenomicFile(kf_id = Some("genomicFile1"), data_type = Some("Super Important type 1"), file_name = Some("File1"))
  val genomicFile2: EGenomicFile = EGenomicFile(kf_id = Some("genomicFile2"), data_type = Some("Super Important type 2"), file_name = Some("File2"))
  val genomicFile3: EGenomicFile = EGenomicFile(kf_id = Some("genomicFile3"), data_type = Some("Super Important type 3"), file_name = Some("File3"))
  val genomicFile4: EGenomicFile = EGenomicFile(kf_id = Some("genomicFile4"), data_type = Some("Super Important type 4"), file_name = Some("File4"))
  val genomicFile5: EGenomicFile = EGenomicFile(kf_id = Some("genomicFile5"), data_type = Some("Super Important type 5"), file_name = Some("File5"))
  val genomicFile6: EGenomicFile = EGenomicFile(kf_id = Some("genomicFile6"), data_type = Some("Super Important type 6"), file_name = Some("File6"))
  val genomicFile7: EGenomicFile = EGenomicFile(kf_id = Some("genomicFile7"), data_type = Some("Super Important type 7"), file_name = Some("File7"))
  val genomicFile8: EGenomicFile = EGenomicFile(kf_id = Some("genomicFile8"), data_type = Some("Super Important type 8"), file_name = Some("File8"))
  val genomicFiles: Seq[EGenomicFile] = Seq(genomicFile1, genomicFile2, genomicFile3, genomicFile4, genomicFile5, genomicFile6, genomicFile7, genomicFile8)

  val eBiospecimenGenomicFile1: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kf_id = Some("eBiospecimenGenomicFile_id_1"), biospecimen_id = Some("biospecimen_id_1"), genomic_file_id = Some("genomicFile1"))
  val eBiospecimenGenomicFile2: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kf_id = Some("eBiospecimenGenomicFile_id_2"), biospecimen_id = Some("biospecimen_id_1_1"), genomic_file_id = Some("genomicFile2"))
  val eBiospecimenGenomicFile3: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kf_id = Some("eBiospecimenGenomicFile_id_3"), biospecimen_id = Some("biospecimen_id_1_2"), genomic_file_id = Some("genomicFile3"))
  val eBiospecimenGenomicFile4: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kf_id = Some("eBiospecimenGenomicFile_id_4"), biospecimen_id = Some("biospecimen_id_2"), genomic_file_id = Some("genomicFile4"))
  val eBiospecimenGenomicFile5: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kf_id = Some("eBiospecimenGenomicFile_id_5"), biospecimen_id = Some("biospecimen_id_3"), genomic_file_id = Some("genomicFile5"))
  val eBiospecimenGenomicFile6: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kf_id = Some("eBiospecimenGenomicFile_id_6"), biospecimen_id = Some("biospecimen_id_4"), genomic_file_id = Some("genomicFile6"))
  val eBiospecimenGenomicFile7: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kf_id = Some("eBiospecimenGenomicFile_id_7"), biospecimen_id = Some("biospecimen_id_1"), genomic_file_id = Some("genomicFile6"))
  val eBiospecimenGenomicFile8: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kf_id = Some("eBiospecimenGenomicFile_id_8"), biospecimen_id = Some("biospecimen_id_3"), genomic_file_id = Some("genomicFile1"))
  val eBiospecimenGenomicFile9: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kf_id = Some("eBiospecimenGenomicFile_id_9"), biospecimen_id = Some("biospecimen_id_5"), genomic_file_id = None)
  val eBiospecimenGenomicFile10: EBiospecimenGenomicFile = EBiospecimenGenomicFile(kf_id = Some("eBiospecimenGenomicFile_id_10"), biospecimen_id = Some("biospecimen_id_6"), genomic_file_id = Some("genomicFile8"))
  val eBiospecimenGenomicFile: Seq[EBiospecimenGenomicFile] = Seq(eBiospecimenGenomicFile1, eBiospecimenGenomicFile2, eBiospecimenGenomicFile3,eBiospecimenGenomicFile4, eBiospecimenGenomicFile5, eBiospecimenGenomicFile6, eBiospecimenGenomicFile7, eBiospecimenGenomicFile8, eBiospecimenGenomicFile9, eBiospecimenGenomicFile10)

  val biospecimenDiagnosis1: EBiospecimenDiagnosis = EBiospecimenDiagnosis(kf_id = Some("bd1"), diagnosis_id = Some("diagnosis_id_1"), biospecimen_id = Some("biospecimen_id_1"))
  val biospecimenDiagnosis2: EBiospecimenDiagnosis = EBiospecimenDiagnosis(kf_id = Some("bd2"), diagnosis_id = Some("diagnosis_id_1_2"), biospecimen_id = Some("biospecimen_id_1_2"))
  val biospecimenDiagnosis3: EBiospecimenDiagnosis = EBiospecimenDiagnosis(kf_id = Some("bd4"), diagnosis_id = Some("diagnosis_id_2"), biospecimen_id = Some("biospecimen_id_2"))
  val biospecimenDiagnosis4: EBiospecimenDiagnosis = EBiospecimenDiagnosis(kf_id = Some("bd3"), diagnosis_id = Some("diagnosis_id_1_3"), biospecimen_id = Some("biospecimen_id_3"))
  val biospecimenDiagnosis: Seq[EBiospecimenDiagnosis] = Seq(biospecimenDiagnosis1, biospecimenDiagnosis2, biospecimenDiagnosis3, biospecimenDiagnosis4)

  val eSequencingExperiment1: ESequencingExperiment = ESequencingExperiment(kf_id = Some("eSeqExp1"), genomic_files = Seq("genomicFile7"), library_prep = Some("this_Prep1"), library_selection = Some("this_Selection1"))
  val eSequencingExperiment = Seq(eSequencingExperiment1)

  val eSequencingExperimentGenomicFile1: ESequencingExperimentGenomicFile = ESequencingExperimentGenomicFile(kf_id = Some("eSeqExpGF1"), sequencing_experiment = Some("eSeqExp1"), genomic_file = Some("genomicFile7"))
  val eSequencingExperimentGenomicFile = Seq(eSequencingExperimentGenomicFile1)

  val duoCode1: DuoCode = DuoCode(id = "duo_id1", shorthand = Some("DuoForShort 1"), label = Some("DuoLabel1"), description = Some("This is a description about duo code"))
  val duoCode2: DuoCode = DuoCode(id = "duo_id2", shorthand = Some("DuoForShort 2"), label = Some("DuoLabel2"), description = Some("This is another description about duo code"))
  val duoCodes = Seq(duoCode1, duoCode2)

  val phenotype_ES1: Phenotype_ES = Phenotype_ES(
    age_at_event_days = Some(15),
    hpo_phenotype_observed = Some("Osteolytic defect of thumb phalanx (HP:0009654)"),
    hpo_phenotype_observed_text = Some("Osteolytic defect of thumb phalanx (HP:0009654)"),
    observed = Some(true)
  )

  val phenotype_ES2: Phenotype_ES = Phenotype_ES(
    age_at_event_days = Some(18),
    hpo_phenotype_observed = Some("Abnormal upper limb bone morphology (HP:0045081)"),
    hpo_phenotype_observed_text = Some("Abnormal upper limb bone morphology (HP:0045081)"),
    observed = Some(true)
  )

  val phenotypes = Seq(phenotype_ES1, phenotype_ES2)
}
