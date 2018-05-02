package io.kf.etl.processors.filecentric.transform.steps

import io.kf.etl.es.models.{FileCentric_ES, Participant_ES}
import io.kf.etl.model.utils._
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class BuildFileCentric(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[FileCentric_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[FileCentric_ES] = {
    import ctx.spark.implicits._

    val files =
      ctx.entityDataset.genomicFiles.joinWith(
        ctx.entityDataset.sequencingExperiments,
        ctx.entityDataset.genomicFiles.col("sequencingExperimentId") === ctx.entityDataset.sequencingExperiments.col("kfId"),
        "left_outer"
      ).map(tuple => {
        val seqExp = PBEntityConverter.ESequencingExperimentToSequencingExperimentES(tuple._2)
        PBEntityConverter.EGenomicFileToGenomicFileES(tuple._1, Some(seqExp))
      })

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
        BiospecimenES_GenomicFileId(
          bio = PBEntityConverter.EBiospecimenToBiospecimenES(tuple._2),
          gfId = tuple._1.kfId.get
        )
      })

    val bio_fullGf =
      files.joinWith(
        bio_gf,
        files.col("kfId") === bio_gf.col("gfId")
      ).map(tuple => {
          BiospecimenES_GenomicFileES(
            bio = tuple._2.bio,
            genomicFile = tuple._1
          )
      })


    bio_fullGf.joinWith(
      bio_par,
      bio_par("bio")("kfId") === bio_fullGf("bio")("kfId"),
      "left_outer"
    ).flatMap(tuple => {
      Option(tuple._2) match {
        case Some(_) => {
          Seq(
            ParticipantES_BiospecimenES_GenomicFileES(
              participant = tuple._2.participant,
              bio = tuple._1.bio,
              genomicFile = tuple._1.genomicFile
            )
          )
        }
        case None => Seq.empty
      }
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

        FileCentric_ES(
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
          isHarmonized = genomicFile.isHarmonized,
          sequencingExperiments = Seq(genomicFile.sequencingExperiment.get)
        )


    })
  }
}
