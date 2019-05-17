package io.kf.etl.processors.filecentric.transform.steps

import io.kf.etl.es.models.{FileCentric_ES, GenomicFile_ES, Participant_ES}
import io.kf.etl.model.utils._
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.context.StepContext
import org.apache.spark.sql.Dataset

class BuildFileCentric(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[FileCentric_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[FileCentric_ES] = {
    import ctx.spark.implicits._

    val fileId_experiments = ctx.entityDataset.sequencingExperiments
      .joinWith(
        ctx.entityDataset.sequencingExperimentGenomicFiles,
        ctx.entityDataset.sequencingExperiments.col("kfId") === ctx.entityDataset.sequencingExperimentGenomicFiles.col("sequencingExperiment"),
        "left_outer"
      )
      .map(tuple => {
        SequencingExperimentES_GenomicFileId(
          sequencingExperiment = PBEntityConverter.ESequencingExperimentToSequencingExperimentES(tuple._1),
          genomicFile = tuple._2.genomicFile
        )
      })
      .groupByKey(_.genomicFile)
      .mapGroups((fileId, iterator) => {
        fileId match {
          case Some(id) => {

            val experiments = iterator.map(_.sequencingExperiment).toSeq
            SequencingExperimentsES_GenomicFileId(
              sequencingExperiments = experiments,
              genomicFile = id
            )
          }
          case None => null
        }
      })
      .filter(_ != null)

    val files: Dataset[GenomicFile_ES] =
      ctx.entityDataset.genomicFiles.joinWith(
        fileId_experiments,
        ctx.entityDataset.genomicFiles.col("kfId") === fileId_experiments.col("genomicFile"),
        "left_outer"
      ).map(tuple => {
        Option(tuple._2) match {
          case Some(_) => {
            PBEntityConverter.EGenomicFileToGenomicFileES(tuple._1, tuple._2.sequencingExperiments)
          }
          case None => PBEntityConverter.EGenomicFileToGenomicFileES(tuple._1, Seq.empty)
        }
      })

    val bio_par: Dataset[BiospecimenES_ParticipantES] =
      participants.flatMap((p: Participant_ES) => p.biospecimens.map(b => BiospecimenES_ParticipantES(b, p)))

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
          bio = tuple._2.bio,
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
          availability = genomicFile.availability,
          accessUrls = genomicFile.accessUrls,
          controlledAccess = genomicFile.controlledAccess,
          dataType = genomicFile.dataType,
          externalId = genomicFile.externalId,
          fileFormat = genomicFile.fileFormat,
          fileName = genomicFile.fileName,
          instrumentModels = genomicFile.instrumentModels,
          isHarmonized = genomicFile.isHarmonized,
          isPairedEnd = genomicFile.isPairedEnd,
          kfId = genomicFile.kfId,
          latestDid = genomicFile.latestDid,
          participants = participants_in_genomicfile.toSeq,
          platforms = genomicFile.platforms,
          referenceGenome = genomicFile.referenceGenome,
          repository = genomicFile.repository,
          sequencingExperiments = genomicFile.sequencingExperiments,
          size = genomicFile.size
        )

      })
  }
}
