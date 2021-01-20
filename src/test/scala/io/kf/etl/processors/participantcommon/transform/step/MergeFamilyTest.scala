package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice._
import io.kf.etl.models.es._
import io.kf.etl.processors.common.converter.EntityConverter.EParticipantToParticipantES
import io.kf.etl.processors.download.transform.DownloadTransformer
import io.kf.etl.processors.participantcommon.transform.step
import io.kf.etl.processors.participantcommon.transform.step.MergeFamily._
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}

class MergeFamilyTest extends FlatSpec with Matchers with WithSparkSession {
  "calculateAvailableDataTypes" should "return a map of datatypes by participant id" in {

    val p1 = EParticipant(kf_id = Some("participant_id_1"))
    val bioSpecimen1 = EBiospecimen(kf_id = Some("biospecimen_id_1"), participant_id = Some("participant_id_1"))
    val biospecimenGeniomicFile11 = EBiospecimenGenomicFile(kf_id = Some("biospeciment_genomic_file_id_11"), biospecimen_id = Some("biospecimen_id_1"), genomic_file_id = Some("genomic_file_id_11"))
    val biospecimenGeniomicFile12 = EBiospecimenGenomicFile(kf_id = Some("biospeciment_genomic_file_id_12"), biospecimen_id = Some("biospecimen_id_1"), genomic_file_id = Some("genomic_file_id_12"))
    val genomicFile11 = EGenomicFile(kf_id = Some("genomic_file_id_11"), data_type = Some("datatype_11"))
    val genomicFile12 = EGenomicFile(kf_id = Some("genomic_file_id_12"), data_type = Some("datatype_12"))

    val p2 = EParticipant(kf_id = Some("participant_id_2"))
    val bioSpecimen2 = EBiospecimen(kf_id = Some("biospecimen_id_2"), participant_id = Some("participant_id_2"))
    val biospecimenGeniomicFile2 = EBiospecimenGenomicFile(kf_id = Some("biospeciment_genomic_file_id_2"), biospecimen_id = Some("biospecimen_id_2"), genomic_file_id = Some("genomic_file_id_2"))
    val genomicFile2 = EGenomicFile(kf_id = Some("genomic_file_id_2"), data_type = Some("datatype_2"))


    //These below should be ignore
    val p3 = EParticipant(kf_id = Some("participant_id__without_file"))
    val biospecimen3 = EBiospecimen(kf_id = Some("biospecimen_id__without_specimen_file"), participant_id = Some("participant_id_2"))
    val biospecimenGeniomicFile3 = EBiospecimenGenomicFile(kf_id = Some("biospeciment_genomic_file_without_genomic_file"), biospecimen_id = Some("biospecimen_id_2"), genomic_file_id = None)
    val biospecimenGeniomicFile4 = EBiospecimenGenomicFile(kf_id = Some("biospeciment_genomic_file_id_duplicate"), biospecimen_id = Some("biospecimen_id_2"), genomic_file_id = Some("genomic_file_id_duplicate"))
    val genomicFile4 = EGenomicFile(kf_id = Some("genomic_file_id_duplicate"), data_type = Some("datatype_2"))

    val entityDataset = buildEntityDataSet(
      participants = Seq(p1, p2, p3),
      biospecimens = Seq(bioSpecimen1, bioSpecimen2, biospecimen3),
      genomicFiles = Seq(genomicFile11, genomicFile12, genomicFile2, genomicFile4),
      biospecimenGenomicFiles = Seq(biospecimenGeniomicFile11, biospecimenGeniomicFile12, biospecimenGeniomicFile2, biospecimenGeniomicFile3, biospecimenGeniomicFile4)
    )


    MergeFamily.calculateAvailableDataTypes(entityDataset).value.mapValues(_.sorted) should contain theSameElementsAs Map(
      "participant_id_1" -> Seq("datatype_11", "datatype_12"),
      "participant_id_2" -> Seq("datatype_2")
    )

  }

  "getAvailableDataTypes" should "return one available datatype of one participant" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    MergeFamily.getAvailableDataTypes(Seq(p1), Map("participant_id_1" -> Seq("datatype_1"))) shouldBe Seq("datatype_1")

  }

  it should "return many available datatypes of one participant" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    MergeFamily.getAvailableDataTypes(Seq(p1), Map("participant_id_1" -> Seq("datatype_1", "datatype_2"))) shouldBe Seq("datatype_1", "datatype_2")

  }

  it should "return available datatypes of many participants" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val p2 = Participant_ES(kf_id = Some("participant_id_2"))
    val p3 = Participant_ES(kf_id = Some("participant_id_3"))
    MergeFamily.getAvailableDataTypes(Seq(p1, p2, p3),
      Map(
        "participant_id_1" -> Seq("datatype_11", "datatype_12"),
        "participant_id_2" -> Seq("datatype_2")
      )
    ) shouldBe Seq("datatype_11", "datatype_12", "datatype_2")

  }

  "process" should "return participants with family composition" in {
    val fmr = Seq(
      EFamilyRelationship(kf_id = Some("MOTHER_CHILD_ID"), participant1 = Some("MOTHER_ID"), participant2 = Some("CHILD_ID"), participant1_to_participant2_relation = Some("Mother"), participant2_to_participant1_relation = Some("Child")),
      EFamilyRelationship(kf_id = Some("FATHER_CHILD_ID"), participant1 = Some("FATHER_ID"), participant2 = Some("CHILD_ID"), participant1_to_participant2_relation = Some("Father"), participant2_to_participant1_relation = Some("Child"))
    )

    val participants = Seq(
      Participant_ES(kf_id = Some("CHILD_ID"), is_proband = Some(true), family_id = Some("FAMILY_ID")),
      Participant_ES(kf_id = Some("MOTHER_ID"), family_id = Some("FAMILY_ID")),
      Participant_ES(kf_id = Some("FATHER_ID"), family_id = Some("FAMILY_ID"))
    )

    val entityDataset = buildEntityDataSet(familyRelationships = fmr)

    import spark.implicits._
    val result = step.MergeFamily(entityDataset, participants.toDS()).collect()
    result should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("CHILD_ID"), is_proband = Some(true), family_id = Some("FAMILY_ID"), family = Some(Family_ES(family_id = Some("FAMILY_ID"), family_compositions = Seq(FamilyComposition_ES(Some("trio"), family_members = Seq(FamilyMember_ES(relationship = Some("mother"), kf_id = Some("MOTHER_ID")), FamilyMember_ES(relationship = Some("father"), kf_id = Some("FATHER_ID")))))))),
      Participant_ES(kf_id = Some("MOTHER_ID"), family_id = Some("FAMILY_ID"), family = Some(Family_ES(family_id = Some("FAMILY_ID"), family_compositions = Seq(FamilyComposition_ES(Some("trio"), family_members = Seq(FamilyMember_ES(relationship = Some("child"), kf_id = Some("CHILD_ID"), is_proband = Some(true)), FamilyMember_ES(relationship = Some("member"), kf_id = Some("FATHER_ID")))))))),
      Participant_ES(kf_id = Some("FATHER_ID"), family_id = Some("FAMILY_ID"), family = Some(Family_ES(family_id = Some("FAMILY_ID"), family_compositions = Seq(FamilyComposition_ES(Some("trio"), family_members = Seq(FamilyMember_ES(relationship = Some("child"), kf_id = Some("CHILD_ID"), is_proband = Some(true)), FamilyMember_ES(relationship = Some("member"), kf_id = Some("MOTHER_ID"))))))))
    )

  }

  "getFlattenedFamilyRelationship" should "return a map of relationship" in {
    val fmr = Seq(
      EFamilyRelationship(kf_id = Some("MOTHER_CHILD_ID"), participant1 = Some("MOTHER_ID"), participant2 = Some("CHILD_ID"), participant1_to_participant2_relation = Some("Mother"), participant2_to_participant1_relation = Some("Child")),
      EFamilyRelationship(kf_id = Some("MOTHER_GRANDFATHER_ID"), participant1 = Some("GRANDFATHER_ID"), participant2 = Some("MOTHER_ID"), participant1_to_participant2_relation = Some("Father"), participant2_to_participant1_relation = Some("Child")),
      EFamilyRelationship(kf_id = Some("FATHER_CHILD_ID"), participant1 = Some("FATHER_ID"), participant2 = Some("CHILD_ID"), participant1_to_participant2_relation = Some("Father"), participant2_to_participant1_relation = Some("Child"))
    )
    val entityDataset = buildEntityDataSet(familyRelationships = fmr)

    val flatenned: Map[String, Seq[(String, String)]] = MergeFamily.getFlattenedFamilyRelationship(entityDataset).value

    flatenned should contain theSameElementsAs Map(
      "CHILD_ID" -> Seq(("MOTHER_ID", "mother"), ("FATHER_ID", "father")),
      "GRANDFATHER_ID" -> Seq(("MOTHER_ID", "child")),
      "MOTHER_ID" -> Seq(("CHILD_ID", "child"), ("GRANDFATHER_ID", "father")),
      "FATHER_ID" -> Seq(("CHILD_ID", "child"))
    )

  }

  "getFamilyComposition" should "return trio" in {
    val fmr = Map(
      "CHILD_ID" -> Seq(("FATHER_ID", "father"), ("MOTHER_ID", "mother")),
      "MOTHER_ID" -> Seq(("FATHER_ID", "member"), ("CHILD_ID", "child")),
      "FATHER_ID" -> Seq(("MOTHER_ID", "other"), ("CHILD_ID", "child"))
    )


    MergeFamily.getFamilyComposition(fmr, Nil) shouldBe Trio
  }

  "getFamilyComposition" should "return trio+" in {
    val fmr = Map(
      "CHILD_ID" -> Seq(("FATHER_ID", "father"), ("MOTHER_ID", "mother"), ("GRANDFATHER_ID", "grandfather")),
      "MOTHER_ID" -> Seq(("FATHER_ID", "member"), ("CHILD_ID", "child")),
      "FATHER_ID" -> Seq(("MOTHER_ID", "other"), ("CHILD_ID", "child")),
      "GRANDFATHER_ID" -> Seq(("MOTHER_ID", "child"))
    )


    MergeFamily.getFamilyComposition(fmr, Nil) shouldBe TrioPlus
  }

  it should "return trio+ - grandparents without proband" in {
    val fmr = Map(
      "CHILD_ID" -> Seq(("GRANDFATHER_ID", "grandfather"), ("GRANDMOTHER_ID", "grandmother"), ("MOTHER_ID", "mother")),
      "MOTHER_ID" -> Seq(("GRANDFATHER_ID", "father"), ("GRANDMOTHER_ID", "mother"), ("CHILD_ID", "child")),
      "GRANDFATHER_ID" -> Seq(("MOTHER_ID", "child"), ("GRANDMOTHER_ID", "other")),
      "GRANDMOTHER_ID" -> Seq(("MOTHER_ID", "child"), ("GRANDFATHER_ID", "other"))
    )

    MergeFamily.getFamilyComposition(fmr, Nil) shouldBe TrioPlus
  }

  it should "return duo" in {
    val fmr = Map(
      "CHILD_ID" -> Seq(("MOTHER_ID", "mother")),
      "MOTHER_ID" -> Seq(("CHILD_ID", "child"))
    )

    MergeFamily.getFamilyComposition(fmr, Nil) shouldBe Duo
  }

  it should "return duo+" in {
    val fmr = Map(
      "CHILD_ID" -> Seq(("MOTHER_ID", "mother")),
      "MOTHER_ID" -> Seq(("CHILD_ID", "child"), ("GRANDUNCLE_ID", "uncle")),
      "GRANDUNCLE_ID" -> Seq(("MOTHER_ID", "other"))
    )

    MergeFamily.getFamilyComposition(fmr, Nil) shouldBe DuoPlus
  }


  it should "return duo+ - grandparents with proband" in {
    val fmr = Map(
      "CHILD_ID" -> Seq(("GRANDFATHER_ID", "grandfather"), ("GRANDMOTHER_ID", "grandmother"), ("MOTHER_ID", "mother")),
      "MOTHER_ID" -> Seq(("GRANDFATHER_ID", "father"), ("GRANDMOTHER_ID", "mother"), ("CHILD_ID", "child")),
      "GRANDFATHER_ID" -> Seq(("MOTHER_ID", "child"), ("GRANDMOTHER_ID", "other")),
      "GRANDMOTHER_ID" -> Seq(("MOTHER_ID", "child"), ("GRANDFATHER_ID", "other"))
    )

    MergeFamily.getFamilyComposition(fmr, probands = Seq("CHILD_ID")) shouldBe DuoPlus
  }

  it should "return proband-only" in {
    val fmr = Map(
      "CHILD_ID" -> Nil
    )

    MergeFamily.getFamilyComposition(fmr, probands = Seq("CHILD_ID")) shouldBe ProbandOnly
  }

  it should "return other" in {
    val fmr = Map(
      "CHILD_ID" -> Seq(("GRANDFATHER_ID", "grandfather")),
      "GRANDFATHER_ID" -> Seq(("CHILD_ID", "other"))
    )

    MergeFamily.getFamilyComposition(fmr, probands = Seq("CHILD_ID")) shouldBe Other
  }


  "getProbandIds" should "return one id" in {
    val participants = Seq(
      Participant_ES(kf_id = Some("CHILD_ID"), is_proband = Some(true), family_id = Some("FAMILY_ID")),
      Participant_ES(kf_id = Some("MOTHER_ID"), family_id = Some("FAMILY_ID"))
    )

    MergeFamily.getProbandIds(participants) shouldBe Seq("CHILD_ID")
  }

  it should "return empty seq" in {
    val participants = Seq(
      Participant_ES(kf_id = Some("CHILD_ID"), family_id = Some("FAMILY_ID")),
      Participant_ES(kf_id = Some("MOTHER_ID"), family_id = Some("FAMILY_ID"))
    )

    MergeFamily.getProbandIds(participants) shouldBe empty
  }

  it should "return empty seq (proband false)" in {
    val participants = Seq(
      Participant_ES(kf_id = Some("CHILD_ID"), is_proband = Some(false), family_id = Some("FAMILY_ID")),
      Participant_ES(kf_id = Some("MOTHER_ID"), family_id = Some("FAMILY_ID"))
    )

    MergeFamily.getProbandIds(participants) shouldBe empty
  }

  it should "return 2 ids" in {
    val participants = Seq(
      Participant_ES(kf_id = Some("CHILD_ID"), is_proband = Some(true), family_id = Some("FAMILY_ID")),
      Participant_ES(kf_id = Some("MOTHER_ID"), is_proband = Some(true), family_id = Some("FAMILY_ID"))
    )

    MergeFamily.getProbandIds(participants) shouldBe Seq("CHILD_ID", "MOTHER_ID")
  }

  "getSharedHpoIds" should "return hpo observed phenotypes shared between participants" in {

    val participants = Seq(
      Participant_ES(kf_id = Some("CHILD_ID"), phenotype = Seq(Phenotype_ES(hpo_phenotype_observed = Some("pheno1")), Phenotype_ES(hpo_phenotype_observed = Some("pheno2")), Phenotype_ES(hpo_phenotype_not_observed = Some("pheno3")))),
      Participant_ES(kf_id = Some("MOTHER_ID"), phenotype = Seq(Phenotype_ES(hpo_phenotype_observed = Some("pheno1")), Phenotype_ES(hpo_phenotype_observed = Some("pheno2")), Phenotype_ES(hpo_phenotype_observed = Some("pheno3"))))
    )

    MergeFamily.getSharedHpoIds(participants) should contain theSameElementsAs Seq(
      "pheno1", "pheno2"
    )

  }

  it should "return empty if there is no share phenotype between participant" in {

    val participants = Seq(
      Participant_ES(kf_id = Some("CHILD_ID"), phenotype = Seq(Phenotype_ES(hpo_phenotype_observed = Some("pheno1")))),
      Participant_ES(kf_id = Some("MOTHER_ID"), phenotype = Seq(Phenotype_ES(hpo_phenotype_observed = Some("pheno2"))))
    )

    MergeFamily.getSharedHpoIds(participants) shouldBe empty
  }
  it should "return empty if one participant have no phenotype" in {

    val participants = Seq(
      Participant_ES(kf_id = Some("CHILD_ID"), phenotype = Seq(Phenotype_ES(hpo_phenotype_observed = Some("pheno1")))),
      Participant_ES(kf_id = Some("MOTHER_ID"), phenotype = Nil)
    )

    MergeFamily.getSharedHpoIds(participants) shouldBe empty
  }

  "getSharedHpoIds" should "return empty family composition if no familyId" in {
    import spark.implicits._

    val participant1 = EParticipant(
      kf_id = Some("Participant1"),
      family_id = None,
      is_proband = Some(true),
      ethnicity = Some("Not Reported Ethnicity"),
      race = Some("Not Reported Race")
    )

    val participant2 = EParticipant(kf_id = Some("Participant2"))

    val participants: Dataset[Participant_ES] = Seq(
      EParticipantToParticipantES(participant1).copy(available_data_types = Seq("stuff"), phenotype = Seq(Phenotype_ES(hpo_phenotype_observed = Some("pheno1"))) ),
      EParticipantToParticipantES(participant2)
    ).toDS()

    val bioSpecimen1 = EBiospecimen(kf_id = Some("biospecimen_id_1"), participant_id = Some("Participant1"))
    val biospecimenGeniomicFile11 = EBiospecimenGenomicFile(kf_id = Some("biospeciment_genomic_file_id_11"), biospecimen_id = Some("biospecimen_id_1"), genomic_file_id = Some("genomic_file_id_11"))
    val genomicFile11 = EGenomicFile(kf_id = Some("genomic_file_id_11"), data_type = Some("stuff"))

    val entityDataset = buildEntityDataSet(
      participants = Seq(participant1, participant2),
      genomicFiles = Seq(genomicFile11),
      biospecimenGenomicFiles = Seq(biospecimenGeniomicFile11),
      biospecimens = Seq(bioSpecimen1)
    )

    val result = MergeFamily(entityDataset, participants)

    val expectedResults = Seq(
      EParticipantToParticipantES(participant1)
        .copy(
          available_data_types = Seq("stuff"),
          phenotype = Seq(Phenotype_ES(hpo_phenotype_observed = Some("pheno1"))),
          family = Some(
            Family_ES(
              family_compositions = Seq(
                FamilyComposition_ES(
                  composition = Some("proband-only")
                )
              )
            )
          )
        ),
      EParticipantToParticipantES(participant2)
        .copy(
          family = Some(
            Family_ES(
              family_compositions = Seq(
                FamilyComposition_ES(
                  composition = Some("other")
                )
              )
            )
          )
        )
    )

    result.collect() should equal(expectedResults)
  }

  it should "merge data types to participants" in {
    import spark.implicits._

    val participant1 = EParticipant(
      kf_id = Some("Participant1"),
      is_proband = Some(true)
    )

    val participant2 = EParticipant(kf_id = Some("Participant2"))

    val participants: Dataset[Participant_ES] = Seq(
      EParticipantToParticipantES(participant1),
      EParticipantToParticipantES(participant2)
    ).toDS()

    val bioSpecimen1 = EBiospecimen(kf_id = Some("biospecimen_id_1"), participant_id = Some("Participant1"))
    val bioSpecimen2 = EBiospecimen(kf_id = Some("biospecimen_id_2"), participant_id = Some("Participant2"))
    val biospecimenGeniomicFile11 = EBiospecimenGenomicFile(kf_id = Some("biospeciment_genomic_file_id_11"), biospecimen_id = Some("biospecimen_id_1"), genomic_file_id = Some("genomic_file_id_11"))
    val biospecimenGeniomicFile12 = EBiospecimenGenomicFile(kf_id = Some("biospeciment_genomic_file_id_12"), biospecimen_id = Some("biospecimen_id_1"), genomic_file_id = Some("genomic_file_id_12"))
    val biospecimenGeniomicFile13 = EBiospecimenGenomicFile(kf_id = Some("biospeciment_genomic_file_id_13"), biospecimen_id = Some("biospecimen_id_1"), genomic_file_id = Some("genomic_file_id_13"))
    val biospecimenGeniomicFile21 = EBiospecimenGenomicFile(kf_id = Some("biospeciment_genomic_file_id_21"), biospecimen_id = Some("biospecimen_id_2"), genomic_file_id = Some("genomic_file_id_21"))
    val genomicFile11 = EGenomicFile(kf_id = Some("genomic_file_id_11"), data_type = Some("Aligned Reads"))
    val genomicFile12 = EGenomicFile(kf_id = Some("genomic_file_id_12"), data_type = Some("Histology Images"))
    val genomicFile13 = EGenomicFile(kf_id = Some("genomic_file_id_13"), data_type = Some("unaligned Reads"))
    val genomicFile21 = EGenomicFile(kf_id = Some("genomic_file_id_21"), data_type = Some("nO DaTa "))

    val entityDataset = buildEntityDataSet(
      participants = Seq(participant1, participant2),
      genomicFiles = Seq(genomicFile11, genomicFile12, genomicFile13, genomicFile21),
      biospecimenGenomicFiles = Seq(biospecimenGeniomicFile11, biospecimenGeniomicFile12, biospecimenGeniomicFile13, biospecimenGeniomicFile21),
      biospecimens = Seq(bioSpecimen1, bioSpecimen2),
      mapOfDataCategory_ExistingTypes = Some(DownloadTransformer.loadCategory_ExistingDataTypes("./src/test/resources/data_category_existing_data.tsv")(spark))
    )

    val result = MergeFamily(entityDataset, participants)

    result.map(s => s.data_category).collect() should contain theSameElementsAs Seq(Seq("Sequencing Reads", "Pathology"), Seq("DNA Methylation"))
  }

}
