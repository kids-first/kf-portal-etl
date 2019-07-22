package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{FamilyComposition_ES, FamilyMember_ES, Family_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity._
import io.kf.etl.processors.common.step.impl.MergeFamily.{Duo, DuoPlus, Trio, TrioPlus}
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.{StepContextUtil, WithSparkSession}
import org.scalatest.{FlatSpec, Matchers}

class MergeFamilyTest extends FlatSpec with Matchers with WithSparkSession {
  "calculateAvailableDataTypes" should "return a map of datatypes by participant id" in {

    val p1 = EParticipant(kfId = Some("participant_id_1"))
    val bioSpecimen1 = EBiospecimen(kfId = Some("biospecimen_id_1"), participantId = Some("participant_id_1"))
    val biospecimenGeniomicFile11 = EBiospecimenGenomicFile(kfId = Some("biospeciment_genomic_file_id_11"), biospecimenId = Some("biospecimen_id_1"), genomicFileId = Some("genomic_file_id_11"))
    val biospecimenGeniomicFile12 = EBiospecimenGenomicFile(kfId = Some("biospeciment_genomic_file_id_12"), biospecimenId = Some("biospecimen_id_1"), genomicFileId = Some("genomic_file_id_12"))
    val genomicFile11 = EGenomicFile(kfId = Some("genomic_file_id_11"), dataType = Some("datatype_11"))
    val genomicFile12 = EGenomicFile(kfId = Some("genomic_file_id_12"), dataType = Some("datatype_12"))

    val p2 = EParticipant(kfId = Some("participant_id_2"))
    val bioSpecimen2 = EBiospecimen(kfId = Some("biospecimen_id_2"), participantId = Some("participant_id_2"))
    val biospecimenGeniomicFile2 = EBiospecimenGenomicFile(kfId = Some("biospeciment_genomic_file_id_2"), biospecimenId = Some("biospecimen_id_2"), genomicFileId = Some("genomic_file_id_2"))
    val genomicFile2 = EGenomicFile(kfId = Some("genomic_file_id_2"), dataType = Some("datatype_2"))


    //These below should be ignore
    val p3 = EParticipant(kfId = Some("participant_id__without_file"))
    val biospecimen3 = EBiospecimen(kfId = Some("biospecimen_id__without_specimen_file"), participantId = Some("participant_id_2"))
    val biospecimenGeniomicFile3 = EBiospecimenGenomicFile(kfId = Some("biospeciment_genomic_file_without_genomic_file"), biospecimenId = Some("biospecimen_id_2"), genomicFileId = None)
    val biospecimenGeniomicFile4 = EBiospecimenGenomicFile(kfId = Some("biospeciment_genomic_file_id_duplicate"), biospecimenId = Some("biospecimen_id_2"), genomicFileId = Some("genomic_file_id_duplicate"))
    val genomicFile4 = EGenomicFile(kfId = Some("genomic_file_id_duplicate"), dataType = Some("datatype_2"))

    val entityDataset = buildEntityDataSet(
      participants = Seq(p1, p2, p3),
      biospecimens = Seq(bioSpecimen1, bioSpecimen2, biospecimen3),
      genomicFiles = Seq(genomicFile11, genomicFile12, genomicFile2, genomicFile4),
      biospecimenGenomicFiles = Seq(biospecimenGeniomicFile11, biospecimenGeniomicFile12, biospecimenGeniomicFile2, biospecimenGeniomicFile3, biospecimenGeniomicFile4)
    )

    val mergeFamily = new MergeFamily(ctx = StepContextUtil.buildContext(entityDataset))

    mergeFamily.calculateAvailableDataTypes(entityDataset).value.mapValues(_.sorted) should contain theSameElementsAs Map(
      "participant_id_1" -> Seq("datatype_11", "datatype_12"),
      "participant_id_2" -> Seq("datatype_2")
    )

  }

  "getAvailableDataTypes" should "return one available datatype of one participant" in {
    val p1 = Participant_ES(kfId = Some("participant_id_1"))
    MergeFamily.getAvailableDataTypes(Seq(p1), Map("participant_id_1" -> Seq("datatype_1"))) shouldBe Seq("datatype_1")

  }

  it should "return many available datatypes of one participant" in {
    val p1 = Participant_ES(kfId = Some("participant_id_1"))
    MergeFamily.getAvailableDataTypes(Seq(p1), Map("participant_id_1" -> Seq("datatype_1", "datatype_2"))) shouldBe Seq("datatype_1", "datatype_2")

  }

  it should "return available datatypes of many participants" in {
    val p1 = Participant_ES(kfId = Some("participant_id_1"))
    val p2 = Participant_ES(kfId = Some("participant_id_2"))
    val p3 = Participant_ES(kfId = Some("participant_id_3"))
    MergeFamily.getAvailableDataTypes(Seq(p1, p2, p3),
      Map(
        "participant_id_1" -> Seq("datatype_11", "datatype_12"),
        "participant_id_2" -> Seq("datatype_2")
      )
    ) shouldBe Seq("datatype_11", "datatype_12", "datatype_2")

  }


  "process" should "return participants with family composition" in {
    val fmr = Seq(
      EFamilyRelationship(kfId = Some("MOTHER_CHILD_ID"), participant1 = Some("MOTHER_ID"), participant2 = Some("CHILD_ID"), participant1ToParticipant2Relation = Some("Mother"), participant2ToParticipant1Relation = Some("Child")),
      EFamilyRelationship(kfId = Some("FATHER_CHILD_ID"), participant1 = Some("FATHER_ID"), participant2 = Some("CHILD_ID"), participant1ToParticipant2Relation = Some("Father"), participant2ToParticipant1Relation = Some("Child"))
    )

    val participants = Seq(
      Participant_ES(kfId = Some("CHILD_ID"), isProband = Some(true), familyId = Some("FAMILY_ID")),
      Participant_ES(kfId = Some("MOTHER_ID"), familyId = Some("FAMILY_ID")),
      Participant_ES(kfId = Some("FATHER_ID"), familyId = Some("FAMILY_ID"))
    )

    val entityDataset = buildEntityDataSet(familyRelationships = fmr)

    val mergeFamily = new MergeFamily(ctx = StepContextUtil.buildContext(entityDataset))
    import spark.implicits._
    val result = mergeFamily.process(participants.toDS()).collect()
    result should contain theSameElementsAs Seq(
      Participant_ES(kfId = Some("CHILD_ID"), isProband = Some(true), familyId = Some("FAMILY_ID"), family = Some(Family_ES(familyId = Some("FAMILY_ID"), familyCompositions = Seq(FamilyComposition_ES(Some("trio"), familyMembers = Seq(FamilyMember_ES(relationship = Some("mother"), kfId = Some("MOTHER_ID")), FamilyMember_ES(relationship = Some("father"), kfId = Some("FATHER_ID")))))))),
      Participant_ES(kfId = Some("MOTHER_ID"), familyId = Some("FAMILY_ID"), family = Some(Family_ES(familyId = Some("FAMILY_ID"), familyCompositions = Seq(FamilyComposition_ES(Some("trio"), familyMembers = Seq(FamilyMember_ES(relationship = Some("child"), kfId = Some("CHILD_ID"), isProband = Some(true)), FamilyMember_ES(relationship = Some("member"), kfId = Some("FATHER_ID")))))))),
      Participant_ES(kfId = Some("FATHER_ID"), familyId = Some("FAMILY_ID"), family = Some(Family_ES(familyId = Some("FAMILY_ID"), familyCompositions = Seq(FamilyComposition_ES(Some("trio"), familyMembers = Seq(FamilyMember_ES(relationship = Some("child"), kfId = Some("CHILD_ID"), isProband = Some(true)), FamilyMember_ES(relationship = Some("member"), kfId = Some("MOTHER_ID"))))))))
    )


  }


  "getFlattenedFamilyRelationship" should "return a map of relationship" in {
    val fmr = Seq(
      EFamilyRelationship(kfId = Some("MOTHER_CHILD_ID"), participant1 = Some("MOTHER_ID"), participant2 = Some("CHILD_ID"), participant1ToParticipant2Relation = Some("Mother"), participant2ToParticipant1Relation = Some("Child")),
      EFamilyRelationship(kfId = Some("MOTHER_GRANDFATHER_ID"), participant1 = Some("GRANDFATHER_ID"), participant2 = Some("MOTHER_ID"), participant1ToParticipant2Relation = Some("Father"), participant2ToParticipant1Relation = Some("Child")),
      EFamilyRelationship(kfId = Some("FATHER_CHILD_ID"), participant1 = Some("FATHER_ID"), participant2 = Some("CHILD_ID"), participant1ToParticipant2Relation = Some("Father"), participant2ToParticipant1Relation = Some("Child"))
    )
    val entityDataset = buildEntityDataSet(familyRelationships = fmr)

    val mergeFamily = new MergeFamily(ctx = StepContextUtil.buildContext(entityDataset))

    val flatenned: Map[String, Seq[(String, String)]] = mergeFamily.getFlattenedFamilyRelationship(entityDataset).value

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

  "getProbandIds" should "return one id" in {
    val participants = Seq(
      Participant_ES(kfId = Some("CHILD_ID"), isProband = Some(true), familyId = Some("FAMILY_ID")),
      Participant_ES(kfId = Some("MOTHER_ID"), familyId = Some("FAMILY_ID"))
    )

    MergeFamily.getProbandIds(participants) shouldBe Seq("CHILD_ID")
  }

  it should "return empty seq" in {
    val participants = Seq(
      Participant_ES(kfId = Some("CHILD_ID"), familyId = Some("FAMILY_ID")),
      Participant_ES(kfId = Some("MOTHER_ID"), familyId = Some("FAMILY_ID"))
    )

    MergeFamily.getProbandIds(participants) shouldBe empty
  }

  it should "return empty seq (proband false)" in {
    val participants = Seq(
      Participant_ES(kfId = Some("CHILD_ID"), isProband = Some(false), familyId = Some("FAMILY_ID")),
      Participant_ES(kfId = Some("MOTHER_ID"), familyId = Some("FAMILY_ID"))
    )

    MergeFamily.getProbandIds(participants) shouldBe empty
  }

  it should "return 2 ids" in {
    val participants = Seq(
      Participant_ES(kfId = Some("CHILD_ID"), isProband = Some(true), familyId = Some("FAMILY_ID")),
      Participant_ES(kfId = Some("MOTHER_ID"), isProband = Some(true), familyId = Some("FAMILY_ID"))
    )

    MergeFamily.getProbandIds(participants) shouldBe Seq("CHILD_ID", "MOTHER_ID")
  }

}
