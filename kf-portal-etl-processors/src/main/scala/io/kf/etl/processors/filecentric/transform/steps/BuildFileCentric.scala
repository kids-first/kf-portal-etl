package io.kf.etl.processors.filecentric.transform.steps

import io.kf.etl.es.models.{FileCentric_ES, Participant_ES, SequencingExperiment_ES}
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

        val seqExp =
          Option(tuple._2) match {
            case Some(_) => Seq(PBEntityConverter.ESequencingExperimentToSequencingExperimentES(tuple._2))
            case None => Seq(SequencingExperiment_ES())
          }

        PBEntityConverter.EGenomicFileToGenomicFileES(tuple._1, seqExp)
      })

    val bio_par =
      ctx.entityDataset.biospecimens
        .joinWith(
          participants,
          participants.col("kfId") === ctx.entityDataset.biospecimens.col("participantId"),
          "left_outer"
        )
        .flatMap(tuple => {
          Option(tuple._2) match {
            case Some(_) => {
              Seq(
                BiospecimenES_ParticipantES(
                  bio = PBEntityConverter.EBiospecimenToBiospecimenES(tuple._1),
                  participant = tuple._2
                )
              )
            }
            case None => Seq.empty
          }
        })

    val bioId_gfId =
      ctx.entityDataset.genomicFiles
        .joinWith(
          ctx.entityDataset.biospecimenGenomicFiles,
          ctx.entityDataset.genomicFiles.col("kfId") === ctx.entityDataset.biospecimenGenomicFiles.col("genomicFileId"),
          "left_outer"
        )
        .map(tuple => {
          BiospecimenId_GenomicFileId(
            gfId = tuple._1.kfId,
            bioId = {
              Option(tuple._2) match {
                case Some(_) => tuple._2.biospecimenId
                case None => null
              }
            }
          )
        })

    val bio_gfId =
      bioId_gfId
        .joinWith(
          ctx.entityDataset.biospecimens,
          bioId_gfId.col("bioId") === ctx.entityDataset.biospecimens.col("kfId"),
          "left_outer"
        )
        .map(tuple => {
          BiospecimenES_GenomicFileId(
            gfId = tuple._1.gfId.get,
            bio = {
              Option(tuple._2) match {
                case Some(_) => PBEntityConverter.EBiospecimenToBiospecimenES(tuple._2)
                case None => null
              }
            }
          )
        })

    val bio_fullGf =
      files
        .joinWith(
          bio_gfId,
          files.col("kfId") === bio_gfId.col("gfId")
        )
        .map(tuple => {
          BiospecimenES_GenomicFileES(
            bio = tuple._2.bio,
            genomicFile = tuple._1
          )
        })


    bio_fullGf
      .joinWith(
        bio_par,
        bio_par("bio")("kfId") === bio_fullGf("bio")("kfId"),
        "left_outer"
      )
      .map(tuple => {

        ParticipantES_BiospecimenES_GenomicFileES(
          participant = {
            Option(tuple._2) match {
              case Some(_) => tuple._2.participant
              case None => null
            }
          },
          bio = tuple._1.bio,
          genomicFile = tuple._1.genomicFile
        )

      })
      .groupByKey(_.genomicFile.kfId)
      .mapGroups((_, iterator) => {

      val seq = iterator.toSeq


      val genomicFile = seq(0).genomicFile

      val participants_in_genomicfile =
        seq.filter(pbg => {
          pbg.bio != null && pbg.participant != null
        }).groupBy(_.participant.kfId.get)
          .map(tuple => {
            val participant = tuple._2(0).participant
            participant.copy(
              biospecimens = tuple._2.map(_.bio)
            )
          })

        FileCentric_ES(
          acl = genomicFile.acl,
          controlledAccess = genomicFile.controlledAccess,
          dataType = genomicFile.dataType,
          externalId = genomicFile.externalId,
          fileFormat = genomicFile.fileFormat,
          fileName = genomicFile.fileName,
          size = genomicFile.size,
          kfId = genomicFile.kfId,
          participants = participants_in_genomicfile.toSeq,
          referenceGenome = genomicFile.referenceGenome,
          isHarmonized = genomicFile.isHarmonized,
          sequencingExperiments = genomicFile.sequencingExperiments,
          latestDid = genomicFile.latestDid
        )

    })
  }
}
