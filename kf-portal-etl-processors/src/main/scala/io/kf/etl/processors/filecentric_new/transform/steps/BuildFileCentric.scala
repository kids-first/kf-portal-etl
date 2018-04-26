package io.kf.etl.processors.filecentric_new.transform.steps

import io.kf.etl.es.models.{FileCentric_ES, Participant_ES}
import io.kf.etl.model.utils.{BiospecimenES_GenomicFileES, BiospecimenES_ParticipantES, ParticipantES_BiospecimenES_GenomicFileES, SeqExpId_FileCentricES}
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class BuildFileCentric(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[SeqExpId_FileCentricES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[SeqExpId_FileCentricES] = {
    import ctx.spark.implicits._

    val bio_par =
      participants.joinWith(ctx.entityDataset.biospecimens, participants.col("kfId") === ctx.entityDataset.biospecimens.col("participantId")).map(tuple => {
        BiospecimenES_ParticipantES(
          bio = PBEntityConverter.EBiospecimenToBiospecimenES(tuple._2),
          participant = tuple._1
        )
      })

    val bio_gf =
      ctx.entityDataset.genomicFiles.joinWith(
        ctx.entityDataset.biospecimens,
        ctx.entityDataset.genomicFiles.col("biospecimenId") === ctx.entityDataset.biospecimens.col("kfId")
      ).map(tuple => {
        BiospecimenES_GenomicFileES(
          bio = PBEntityConverter.EBiospecimenToBiospecimenES(tuple._2),
          genomicFile = PBEntityConverter.EGenomicFileToGenomicFileES(tuple._1)
        )
      })


    bio_gf.joinWith(
      bio_par,
      bio_par("bio")("kfId") === bio_gf("bio")("kfId"),
      "left"
    ).map(tuple => {
      ParticipantES_BiospecimenES_GenomicFileES(
        participant = tuple._2.participant,
        bio = tuple._1.bio,
        genomicFile = tuple._1.genomicFile
      )
    }).groupByKey(_.genomicFile.kfId).mapGroups((_, iterator) => {
      val seq = iterator.toSeq


      val genomicFile = seq(0).genomicFile
      val participants_in_genomicfile =
        seq.groupBy(_.participant.kfId.get).map(tuple => {
          val participant = tuple._2(0).participant
          participant.copy(
            biospecimens = tuple._2.map(_.bio)
          )
        })

      SeqExpId_FileCentricES(
        seqExpId = genomicFile.sequencingExperimentId.get,
        filecentric = FileCentric_ES(
          controlledAccess = genomicFile.controlledAccess,
          createdAt = genomicFile.createdAt,
          dataType = genomicFile.dataType,
          fileFormat = genomicFile.fileFormat,
          fileName = genomicFile.fileName,
          size = genomicFile.size,
          kfId = genomicFile.kfId,
          modifiedAt = genomicFile.modifiedAt,
          participants = participants_in_genomicfile.toSeq,
          referenceGenome = genomicFile.referenceGenome,
          isHarmonized = genomicFile.isHarmonized
        )
      )

    })
  }
}
