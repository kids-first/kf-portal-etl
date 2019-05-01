package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.external.dataservice.entity._
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


}