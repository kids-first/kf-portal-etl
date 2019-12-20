package io.kf.etl.processors.featurecentric.transform

import io.kf.etl.models.dataservice.{EBiospecimen, EBiospecimenGenomicFile, EGenomicFile, ESequencingExperiment, ESequencingExperimentGenomicFile}
import io.kf.etl.models.es.{BiospecimenCombined_ES, FileCentric_ES, GenomicFile_ES, ParticipantCombined_ES, Participant_ES}
import io.kf.etl.models.internal.{BiospecimenCombinedES_GenomicFileId, BiospecimenES_GenomicFileES, BiospecimenES_ParticipantES, BiospecimenId_GenomicFileId, ParticipantES_BiospecimenES_GenomicFileES, SequencingExperimentES_GenomicFileId, SequencingExperimentsES_GenomicFileId}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object FeatureCentricTransformer {
  val spark: SparkSession = SparkSession.builder.getOrCreate()
  import spark.implicits._

  def participant(entityDataset:EntityDataSet, participants: Dataset[Participant_ES]): Dataset[ParticipantCombined_ES] = {

    val fileId_experiments: Dataset[SequencingExperimentsES_GenomicFileId] =
      joinFileId_To_SeqExperiments(entityDataset.sequencingExperiments, entityDataset.sequencingExperimentGenomicFiles )

    val files: Dataset[GenomicFile_ES] =
      joinGenomicFiles_To_SequencingExperimentFileId(fileId_experiments, entityDataset.genomicFiles)

    val bio_gf: Dataset[(EBiospecimen, Seq[GenomicFile_ES])] =
      joinGenomicFiles_To_Biospecimen(entityDataset.biospecimens, entityDataset.biospecimenGenomicFiles, files)

    participants.joinWith(
      bio_gf,
      participants.col("kf_id") === bio_gf.col("_1.participantId"),
      "left_outer"
    ).groupByKey { case (participant, _) => participant.kf_id.get }
      .mapGroups { case (_, groupsIterator) =>
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val bioFiles: Seq[BiospecimenCombined_ES] = groups.collect {
          case (_, (biospecimen, gfiles)) if biospecimen != null => EntityConverter.EBiospecimenToBiospecimenCombinedES(biospecimen, gfiles)
        }
        val gfiles: Seq[GenomicFile_ES] = bioFiles.flatMap(_.genomic_files)


        ParticipantCombined_ES(
          affected_status = participant.affected_status,
          alias_group = participant.alias_group,
          available_data_types = participant.available_data_types,
          biospecimens = bioFiles,
          diagnoses = participant.diagnoses,
          diagnosis_category = participant.diagnosis_category,
          ethnicity = participant.ethnicity,
          external_id = participant.external_id,
          family = participant.family,
          family_id = participant.family_id,
          files = gfiles,
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

  def file(entityDataset: EntityDataSet, participants: Dataset[Participant_ES]): Dataset[FileCentric_ES] = {

    val fileId_experiments =
      joinFileId_To_SeqExperiments(entityDataset.sequencingExperiments, entityDataset.sequencingExperimentGenomicFiles)

    val files: Dataset[GenomicFile_ES] =
      joinGenomicFiles_To_SequencingExperimentFileId(fileId_experiments, entityDataset.genomicFiles)

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
                case Some(_) => EntityConverter.EBiospecimenToBiospecimenCombinedES(tuple._2) //FIXME add files per biospecimen
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
        .as[(GenomicFile_ES, BiospecimenCombinedES_GenomicFileId)]
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
          access_urls = genomicFile.access_urls,
          availability = genomicFile.availability,
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
          participants = participants_in_genomicfile.toSeq, //FixME new BiospecimenCombined_ES
          platforms = genomicFile.platforms,
          reference_genome = genomicFile.reference_genome,
          repository = genomicFile.repository,
          sequencing_experiments = genomicFile.sequencing_experiments,
          size = genomicFile.size
        )

      })
  }

  //TODO Should be generic Type -- Join FileId to A / check if col("kfId") exist for A (of user an upper class/trait)
  private def joinFileId_To_SeqExperiments(
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

  private def joinGenomicFiles_To_SequencingExperimentFileId(
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

  private def joinGenomicFiles_To_Biospecimen(
                                               biospecimen: Dataset[EBiospecimen],
                                               biospecimentGenomicFile: Dataset[EBiospecimenGenomicFile],
                                               genomicFiles: Dataset[GenomicFile_ES]
                                             ): Dataset[(EBiospecimen, Seq[GenomicFile_ES])] = {
    val bioSpec_GFs: Dataset[(EBiospecimen, GenomicFile_ES)] =
      biospecimen
        .joinWith(
          biospecimentGenomicFile,
          biospecimen.col("kfId") === biospecimentGenomicFile.col("biospecimenId"),
          "left_outer"
        )
        .as[(EBiospecimen, EBiospecimenGenomicFile)]
        .toDF("eBiospecimen", "eBiospecimenGenomicFile")
        .joinWith(
          genomicFiles,
          $"eBiospecimenGenomicFile.genomicFileId" === genomicFiles("kf_id")
        )
        .as[((EBiospecimen, EBiospecimenGenomicFile), GenomicFile_ES)]
        .map { case ((biospecimen, _), file) => (biospecimen, file) }

    bioSpec_GFs.groupByKey { case (biospecimen, _) => biospecimen.kfId }
      .mapGroups { case (_, groupsIterator) =>
        val groups = groupsIterator.toSeq
        val biospecimen: EBiospecimen = groups.head._1
        val files: Seq[GenomicFile_ES] = groups.map(_._2)
        (biospecimen, files)
      }
  }
}
