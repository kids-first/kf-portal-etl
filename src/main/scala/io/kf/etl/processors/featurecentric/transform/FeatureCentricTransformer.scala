package io.kf.etl.processors.featurecentric.transform

import io.kf.etl.models.dataservice.{EBiospecimen, EBiospecimenGenomicFile, EGenomicFile, ESequencingExperiment, ESequencingExperimentGenomicFile}
import io.kf.etl.models.es.{Biospecimen_ES, FileCentric_ES, GenomicFile_ES, ParticipantCentric_ES, Participant_ES}
import io.kf.etl.models.internal.{BiospecimenCombinedES_GenomicFileId, BiospecimenES_GenomicFileES, BiospecimenES_ParticipantES, BiospecimenId_GenomicFileId, ParticipantES_BiospecimenES_GenomicFileES, SequencingExperimentES_GenomicFileId, SequencingExperimentsES_GenomicFileId}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object FeatureCentricTransformer {
  val spark: SparkSession = SparkSession.builder.getOrCreate()
  import spark.implicits._

  def participantCentric(entityDataset:EntityDataSet, participants: Dataset[Participant_ES]): Dataset[ParticipantCentric_ES] = {

    val fileId_experiments: Dataset[SequencingExperimentsES_GenomicFileId] =
      joinFileId_To_SeqExperiments(entityDataset.sequencingExperiments, entityDataset.sequencingExperimentGenomicFiles )

    val files: Dataset[GenomicFile_ES] =
      joinGenomicFiles_To_SequencingExperimentFileId(fileId_experiments, entityDataset.genomicFiles)

    val bio_gf: Dataset[(EBiospecimen, Seq[GenomicFile_ES])] =
      joinGenomicFiles_To_Biospecimen(entityDataset.biospecimens, entityDataset.biospecimenGenomicFiles, files)

    println("biogf")
    bio_gf.collect().foreach(println)

    participants.joinWith(
      bio_gf,
      participants.col("kf_id") === bio_gf.col("_1.participantId"),
      "left_outer"
    ).groupByKey { case (participant, _) => participant.kf_id }
      .mapGroups { case (_, groupsIterator) =>
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val bioFiles: Seq[Biospecimen_ES] = groups.collect {
          case (_, (biospecimen, gfiles)) if biospecimen != null => EntityConverter.EBiospecimenToBiospecimenCombinedES(biospecimen, gfiles)
        }
        val gfiles: Seq[GenomicFile_ES] = bioFiles.flatMap(_.genomic_files)

        participant_ES_to_ParticipantCentric_ES(participant,gfiles, bioFiles)
      }

  }

  def fileCentric(entityDataset: EntityDataSet, participants: Dataset[Participant_ES]): Dataset[FileCentric_ES] = {

    val fileId_experiments =
      joinFileId_To_SeqExperiments(entityDataset.sequencingExperiments, entityDataset.sequencingExperimentGenomicFiles)

    val files: Dataset[GenomicFile_ES] =
      joinGenomicFiles_To_SequencingExperimentFileId(fileId_experiments, entityDataset.genomicFiles)

    val filesBio = files.joinWith(
      entityDataset.biospecimenGenomicFiles,
      entityDataset.biospecimenGenomicFiles.col("genomicFileId") === files("kf_id"),
      "left_outer"
    )

    val gF_bio = filesBio.joinWith(
        entityDataset.biospecimens,
        filesBio.col("_2.biospecimenId") === entityDataset.biospecimens("kfId"),
    "left_outer"
    ).map(a => (a._1._1, a._2))

    println("GENOMICFILE_BIOSPECIMEN")
    gF_bio.show(false)

    println("PARTICIPANTS")
    participants.show(false)

    println("GF_BIO + Participant")
    gF_bio.joinWith(
      participants,
      participants.col("kf_id") === gF_bio.col("_2.participantId"),
      "left_outer"
    ).show(false)


    println("File_Centric")
    gF_bio.joinWith(
      participants,
      participants.col("kf_id") === gF_bio.col("_2.participantId"),
      "left_outer"
    )
      .map(a => (a._1._1, a._2))
      .groupByKey(_._1)
      .mapGroups { case (file, groupsIterator) =>
        (file, groupsIterator.toSeq.filter{case (_, p)=> p != null}.map(_._2))
      }
      .map{a =>
        genomicFile_ES_to_FileCentric(a._1, a._2)}.show(false)

    gF_bio.joinWith(
      participants,
      participants.col("kf_id") === gF_bio.col("_2.participantId"),
      "left_outer"
    )
      .map(a => (a._1._1, a._2))
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
          $"eBiospecimenGenomicFile.genomicFileId" === genomicFiles("kf_id"),
          "left_outer"
        )
        .as[((EBiospecimen, EBiospecimenGenomicFile), GenomicFile_ES)]
        .map { case ((biospecimen, _), file) => (biospecimen, file) }

    bioSpec_GFs.groupByKey { case (biospecimen, _) => biospecimen.kfId }
      .mapGroups { case (_, groupsIterator) =>
        val groups = groupsIterator.toSeq
        val biospecimen: EBiospecimen = groups.head._1
        val files: Seq[GenomicFile_ES] = groups.collect{ case(_, f) if f != null => f }
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
