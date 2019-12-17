package io.kf.etl.processors.filecentric.transform

import io.kf.etl.models.es.{FileCentric_ES, GenomicFile_ES, Participant_ES}
import io.kf.etl.models.internal.{BiospecimenES_GenomicFileES, BiospecimenES_ParticipantES, BiospecimenId_GenomicFileId, SequencingExperimentsES_GenomicFileId, _}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object FileCentricTransformer{
  def apply(entityDataset: EntityDataSet, participants: Dataset[Participant_ES])(implicit spark:SparkSession): Dataset[FileCentric_ES] = {

    import spark.implicits._
    val fileId_experiments = entityDataset.sequencingExperiments
      .joinWith(
        entityDataset.sequencingExperimentGenomicFiles,
        entityDataset.sequencingExperiments.col("kfId") === entityDataset.sequencingExperimentGenomicFiles.col("sequencingExperiment"),
        "left_outer"
      )
      .map(tuple => {
        SequencingExperimentES_GenomicFileId(
          sequencingExperiment = EntityConverter.ESequencingExperimentToSequencingExperimentES(tuple._1),
          genomicFileId = tuple._2.genomicFile
        )
      })
      .groupByKey(_.genomicFileId)
      .mapGroups((fileId, iterator) => {
        fileId match {
          case Some(id) =>

            val experiments = iterator.map(_.sequencingExperiment).toSeq
            SequencingExperimentsES_GenomicFileId(
              sequencingExperiments = experiments,
              genomicFileId = id
            )
          case None => null
        }
      })
      .filter(_ != null)

    val files: Dataset[GenomicFile_ES] =
      entityDataset.genomicFiles.joinWith(
        fileId_experiments,
        entityDataset.genomicFiles.col("kfId") === fileId_experiments.col("genomicFile"),
        "left_outer"
      ).map(tuple => {
        Option(tuple._2) match {
          case Some(_) =>
            EntityConverter.EGenomicFileToGenomicFileES(tuple._1, tuple._2.sequencingExperiments)
          case None => EntityConverter.EGenomicFileToGenomicFileES(tuple._1, Seq.empty)
        }
      })

    val bio_par: Dataset[BiospecimenES_ParticipantES] =
      participants.flatMap((p: Participant_ES) => p.biospecimens.map(b => BiospecimenES_ParticipantES(b, p)))

    val bioId_gfId =
      entityDataset.genomicFiles
        .joinWith(
          entityDataset.biospecimenGenomicFiles,
          entityDataset.genomicFiles.col("kfId") === entityDataset.biospecimenGenomicFiles.col("genomicFileId"),
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
          entityDataset.biospecimens,
          bioId_gfId.col("bioId") === entityDataset.biospecimens.col("kfId"),
          "left_outer"
        )
        .map(tuple => {
          BiospecimenES_GenomicFileId(
            gfId = tuple._1.gfId.get,
            bio = {
              Option(tuple._2) match {
                case Some(_) => EntityConverter.EBiospecimenToBiospecimenES(tuple._2)
                case None => null
              }
            }
          )
        })

    val bio_fullGf: Dataset[BiospecimenES_GenomicFileES] =
      files
        .joinWith(
          bio_gfId,
          files.col("kf_id") === bio_gfId.col("gfId")
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
        bio_par("bio")("kf_id") === bio_fullGf("bio")("kf_id"),
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
          bio = Option(tuple._2) match {
            case Some(_) => tuple._2.bio
            case None => null
          },
          genomicFile = tuple._1.genomicFile
        )

      })
      .groupByKey(_.genomicFile.kf_id)
      .mapGroups((_, iterator) => {

        val seq = iterator.toSeq


        val genomicFile = seq.head.genomicFile

        val participants_in_genomicfile =
          seq.filter(pbg => {
            pbg.bio != null && pbg.participant != null
          }).groupBy(_.participant.kf_id.get)
            .map(tuple => {
              val participant = tuple._2.head.participant
              participant.copy(
                biospecimens = tuple._2.map(_.bio)
              )
            })

        FileCentric_ES(
          acl = genomicFile.acl,
          availability = genomicFile.availability,
          access_urls = genomicFile.access_urls,
          controlled_access = genomicFile.controlled_access,
          data_type = genomicFile.data_type,
          external_id = genomicFile.external_id,
          file_format = genomicFile.file_format,
          file_name = genomicFile.file_name,
          instrument_models = genomicFile.instrument_models,
          is_harmonized = genomicFile.is_harmonized,
          is_paired_end = genomicFile.is_paired_end,
          kf_id = genomicFile.kf_id,
          latest_did = genomicFile.latest_did,
          participants = participants_in_genomicfile.toSeq,
          platforms = genomicFile.platforms,
          reference_genome = genomicFile.reference_genome,
          repository = genomicFile.repository,
          sequencing_experiments = genomicFile.sequencing_experiments,
          size = genomicFile.size
        )

      })

  }
}
