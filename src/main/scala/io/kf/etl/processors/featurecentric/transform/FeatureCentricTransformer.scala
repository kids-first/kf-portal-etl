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
    ).groupByKey { case (participant, _) => participant.kf_id }
      .mapGroups { case (_, groupsIterator) =>
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val bioFiles: Seq[BiospecimenCombined_ES] = groups.collect {
          case (_, (biospecimen, gfiles)) if biospecimen != null => EntityConverter.EBiospecimenToBiospecimenCombinedES(biospecimen, gfiles)
        }
        val gfiles: Seq[GenomicFile_ES] = bioFiles.flatMap(_.genomic_files)

        participant_ES_to_ParticipantCentric_ES(participant,gfiles, bioFiles)
      }

  }

  def file(entityDataset: EntityDataSet, participants: Dataset[Participant_ES]): Dataset[FileCentric_ES] = {

    val fileId_experiments =
      joinFileId_To_SeqExperiments(entityDataset.sequencingExperiments, entityDataset.sequencingExperimentGenomicFiles)

    val files: Dataset[GenomicFile_ES] =
      joinGenomicFiles_To_SequencingExperimentFileId(fileId_experiments, entityDataset.genomicFiles)

    files.show(false)

    val ebio_gfs = joinGenomicFiles_To_Biospecimen(
      entityDataset.biospecimens,
      entityDataset.biospecimenGenomicFiles,
      files
    )

    val prarticipants_Bio =
      ebio_gfs
        .joinWith(
          participants,
          ebio_gfs.col("_1.participantId") === participants.col("kf_id"),
          "left_outer")
        .map{ case(a, p) => (p, EntityConverter.EBiospecimenToBiospecimenCombinedES(a._1, a._2).genomic_files) }



    prarticipants_Bio
      .flatMap(a => a._2.map(gfs => (gfs, a._1)))
      .filter(_._1 != null)
      .groupByKey(_._1)
        .mapGroups { case (file, groupsIterator) =>
          (file, groupsIterator.toSeq.filter{case (_, p)=> p != null}.map(_._2))
        }
        .map{a =>
          genomicFile_ES_to_FileCentric(a._1, a._2)}
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
                                                       biospecimens: Seq[BiospecimenCombined_ES]
                                                     ): ParticipantCombined_ES = {
    ParticipantCombined_ES(
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
