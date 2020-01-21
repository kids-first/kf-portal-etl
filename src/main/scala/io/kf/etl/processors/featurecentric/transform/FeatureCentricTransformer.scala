package io.kf.etl.processors.featurecentric.transform

import io.kf.etl.models.dataservice.{EGenomicFile, ESequencingExperiment, ESequencingExperimentGenomicFile}
import io.kf.etl.models.es._
import io.kf.etl.models.internal._
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object FeatureCentricTransformer {
  val spark: SparkSession = SparkSession.builder.getOrCreate()
  import spark.implicits._

  def participantCentric(entityDataset:EntityDataSet, participants: Dataset[Participant_ES]): Dataset[ParticipantCentric_ES] = {
    import org.apache.spark.sql.functions.{collect_list, explode_outer, first}

    val fileId_experiments: Dataset[SequencingExperimentsES_GenomicFileId] =
      joinFileIdToSeqExperiments(entityDataset.sequencingExperiments, entityDataset.sequencingExperimentGenomicFiles )

    val files: Dataset[GenomicFile_ES] =
      joinGenomicFilesToSequencingExperimentFileId(fileId_experiments, entityDataset.genomicFiles)

    val bioId_GF = entityDataset.biospecimenGenomicFiles.joinWith(
      files,
      $"genomicFileId" === $"kf_id",
      "left_outer"
    )
      .select("_1.biospecimenId", "_2")
      .withColumnRenamed("_2", "genomicFiles")
      .groupBy(
        "biospecimenId"
      )
      .agg(collect_list("genomicFiles") as "genomicFiles")
      .as[(String, Seq[GenomicFile_ES])]


    val participantExploded = participants
      .withColumn("biospecimen", explode_outer($"biospecimens"))

    participantExploded.joinWith(
      bioId_GF,
      participantExploded.col("biospecimen.kf_id") ===  bioId_GF.col("biospecimenId"),
      "left_outer"
    ).select($"_1" as "participant", $"_2.genomicFiles" as "genomicFiles", $"_1.biospecimen" as "biospecimen")
      .as[(Participant_ES, Seq[GenomicFile_ES], Biospecimen_ES)]
      .map{
        case(p, gfs, null) => (p, gfs, null)
        case(p, gfs, b) => (p, gfs, b.copy(genomic_files = gfs))
      }
      .withColumnRenamed("_1", "participant")
      .withColumnRenamed("_2", "genomicFiles")
      .withColumnRenamed("_3", "biospecimens")
      .groupBy("participant.kf_id")
      .agg(
        first("participant") as "participant",
        collect_list("genomicFiles") as "genomicFiles",
        collect_list("biospecimens") as "biospecimens")
      .drop("kf_id")
      .as[(Participant_ES, Seq[Seq[GenomicFile_ES]], Seq[Biospecimen_ES])]
      .map{ case(p, gfs, b) =>  participant_ES_to_ParticipantCentric_ES(p, gfs.flatten.distinct, b) }
  }

  def fileCentric(entityDataset: EntityDataSet, participants: Dataset[Participant_ES]): Dataset[FileCentric_ES] = {
    import spark.implicits._
    val fileId_experiments =
      joinFileIdToSeqExperiments(entityDataset.sequencingExperiments, entityDataset.sequencingExperimentGenomicFiles)

    val files: Dataset[GenomicFile_ES] =
      joinGenomicFilesToSequencingExperimentFileId(fileId_experiments, entityDataset.genomicFiles)

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
          BiospecimenCombinedES_GenomicFileId(
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

        genomicFile_ES_to_FileCentric(genomicFile, participants_in_genomicfile.toSeq)

      })

  }

  private def joinFileIdToSeqExperiments(
                                          eSequencingExperiment: Dataset[ESequencingExperiment],
                                          eSequencingExperimentGenomicFile: Dataset[ESequencingExperimentGenomicFile]
                                        ): Dataset[SequencingExperimentsES_GenomicFileId] = {
    eSequencingExperiment
      .joinWith(
        eSequencingExperimentGenomicFile,
        eSequencingExperiment.col("kfId") === eSequencingExperimentGenomicFile.col("sequencingExperiment"),
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
              genomicFileId = id,
              sequencingExperiments = experiments
            )
          case None => null
        }
      })
      .filter(_ != null)
  }

  private def joinGenomicFilesToSequencingExperimentFileId(
                                            sequencingExperimentsES_GenomicFileId: Dataset[SequencingExperimentsES_GenomicFileId],
                                            genomicFile: Dataset[EGenomicFile]
                                          ): Dataset[GenomicFile_ES] = {
    genomicFile.joinWith(
      sequencingExperimentsES_GenomicFileId,
      genomicFile.col("kfId") === sequencingExperimentsES_GenomicFileId.col("genomicFileId"),
      "left_outer"
    ).map(tuple => {
      Option(tuple._2) match {
        case Some(_) =>
          EntityConverter.EGenomicFileToGenomicFileES(tuple._1, tuple._2.sequencingExperiments)
        case None => EntityConverter.EGenomicFileToGenomicFileES(tuple._1, Seq.empty)
      }
    })
  }

  private def genomicFile_ES_to_FileCentric(
                                             genomicFile: GenomicFile_ES,
                                             participants: Seq[Participant_ES]
                                           ): FileCentric_ES = {
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
      participants = participants,
      platforms = genomicFile.platforms,
      reference_genome = genomicFile.reference_genome,
      repository = genomicFile.repository,
      sequencing_experiments = genomicFile.sequencing_experiments,
      size = genomicFile.size
    )
  }
  private def participant_ES_to_ParticipantCentric_ES(
                                                       participant: Participant_ES,
                                                       files: Seq[GenomicFile_ES],
                                                       biospecimens: Seq[Biospecimen_ES]
                                                     ): ParticipantCentric_ES = {

    ParticipantCentric_ES(
      affected_status = participant.affected_status,
      alias_group = participant.alias_group,
      available_data_types = participant.available_data_types,
      biospecimens = biospecimens,
      diagnoses = participant.diagnoses,
      diagnosis_category = participant.diagnosis_category,
      ethnicity = participant.ethnicity,
      external_id = participant.external_id,
      family = participant.family,
      family_id = participant.family_id,
      files = files,
      gender = participant.gender,
      is_proband = participant.is_proband,
      kf_id = participant.kf_id,
      outcome = participant.outcome,
      phenotype = participant.phenotype,
      race = participant.race,
      study = participant.study
    )
  }
}
