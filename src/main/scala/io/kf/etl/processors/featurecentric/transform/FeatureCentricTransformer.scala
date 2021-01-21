package io.kf.etl.processors.featurecentric.transform

import io.kf.etl.models.dataservice.{EGenomicFile, ESequencingExperiment, ESequencingExperimentGenomicFile}
import io.kf.etl.models.es._
import io.kf.etl.models.internal._
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.functions.{collect_list, explode_outer, first}
import org.apache.spark.sql.{Dataset, SparkSession}

object FeatureCentricTransformer {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  import spark.implicits._

  def participantCentric(entityDataset: EntityDataSet, participants: Dataset[Participant_ES]): Dataset[ParticipantCentric_ES] = {

    val fileId_experiments: Dataset[SequencingExperimentsES_GenomicFileId] =
      joinFileIdToSeqExperiments(entityDataset.sequencingExperiments, entityDataset.sequencingExperimentGenomicFiles)

    val files: Dataset[GenomicFile_ES] =
      joinGenomicFilesToSequencingExperimentFileId(fileId_experiments, entityDataset.genomicFiles)

    val bioId_GF = entityDataset.biospecimenGenomicFiles.joinWith(
      files,
      $"genomic_file_id" === files("kf_id"),
      "left_outer"
    )
      .select("_1.biospecimen_id", "_2")
      .withColumnRenamed("_2", "genomic_files")
      .groupBy(
        "biospecimen_id"
      )
      .agg(collect_list("genomic_files") as "genomic_files")
      .as[(String, Seq[GenomicFile_ES])]


    val participantExploded = participants
      .withColumn("biospecimen", explode_outer($"biospecimens"))

    participantExploded.joinWith(
      bioId_GF,
      participantExploded.col("biospecimen.kf_id") === bioId_GF.col("biospecimen_id"),
      "left_outer"
    ).select($"_1" as "participant", $"_2.genomic_files" as "genomic_files", $"_1.biospecimen" as "biospecimen")
      .as[(Participant_ES, Seq[GenomicFile_ES], Biospecimen_ES)]
      .map {
        case (p, gfs, null) => (p, gfs, null)
        case (p, gfs, b) => (p, gfs, b.copy(genomic_files = gfs))
      }
      .withColumnRenamed("_1", "participant")
      .withColumnRenamed("_2", "genomic_files")
      .withColumnRenamed("_3", "biospecimens")
      .groupBy("participant.kf_id")
      .agg(
        first("participant") as "participant",
        collect_list("genomic_files") as "genomic_files",
        collect_list("biospecimens") as "biospecimens")
      .drop("kf_id")
      .as[(Participant_ES, Seq[Seq[GenomicFile_ES]], Seq[Biospecimen_ES])]
      .map { case (p, gfs, b) => participant_ES_to_ParticipantCentric_ES(p, gfs.flatten.distinct, b) }
  }

  def fileCentric(entityDataset: EntityDataSet, participants: Dataset[Participant_ES]): Dataset[FileCentric_ES] = {
    import spark.implicits._
    val fileId_experiments =
      joinFileIdToSeqExperiments(entityDataset.sequencingExperiments, entityDataset.sequencingExperimentGenomicFiles)

    val files: Dataset[GenomicFile_ES] =
      joinGenomicFilesToSequencingExperimentFileId(fileId_experiments, entityDataset.genomicFiles)

    val participantExploded = participants
      .withColumn("biospecimen", explode_outer($"biospecimens"))

    files.joinWith(
      entityDataset.biospecimenGenomicFiles,
      files.col("kf_id") === $"genomic_file_id",
      "left_outer"
    )
      .select("_1", "_2.biospecimen_id")
      .as[(GenomicFile_ES, String)]
      .joinWith(
        participantExploded,
        $"biospecimen_id" === participantExploded.col("biospecimen.kf_id"),
        "left_outer"
      )
      .select(
        $"_1._1" as "genomic_file",
        $"_2" as "participant",
        $"_2.biospecimen" as "biospecimen"
      )
      .groupBy(
        "genomic_file", "participant.kf_id"
      )
      .agg(
        first("participant") as "participant",
        collect_list("biospecimen") as "biospecimens"
      )
      .drop("kf_id")
      .as[(GenomicFile_ES, Participant_ES, Seq[Biospecimen_ES])]
      .map {
        case (gf, null, _) => (gf, null)
        case (gf, p, bs) => (gf, p.copy(biospecimens = bs))
      }
      .withColumnRenamed("_1", "genomic_file")
      .withColumnRenamed("_2", "participant")
      .groupBy("genomic_file")
      .agg(
        collect_list("participant") as "participants"
      )
      .as[(GenomicFile_ES, Seq[Participant_ES])]
      .map {
        case (gf, ps) => genomicFile_ES_to_FileCentric(gf, ps)
      }

  }

  def studyCentric(entityDataset: EntityDataSet, studyId: String, participant_count: Long, file_count: Long, family_count: Long): Dataset[StudyCentric_ES] = {
    import spark.implicits._

    val study = entityDataset.studies.filter(s => s.kf_id match {
      case Some(study) => study == studyId
      case None => false
    })
    study.map(s => StudyCentric_ES(
      kf_id = s.kf_id,
      name = s.name,
      external_id = s.external_id,
      data_access_authority = s.data_access_authority,
      code = s.code,
      domain = s.domain,
      program = s.program,
      participant_count = Some(participant_count),
      file_count = Some(file_count),
      family_count = Some(family_count)
    ))

  }

  private def joinFileIdToSeqExperiments(
                                          eSequencingExperiment: Dataset[ESequencingExperiment],
                                          eSequencingExperimentGenomicFile: Dataset[ESequencingExperimentGenomicFile]
                                        ): Dataset[SequencingExperimentsES_GenomicFileId] = {
    eSequencingExperiment
      .joinWith(
        eSequencingExperimentGenomicFile,
        eSequencingExperiment.col("kf_id") === eSequencingExperimentGenomicFile.col("sequencing_experiment"),
        "left_outer"
      )
      .map { case (sequencingExperiment, sequencingExperimentGenomicFile) =>
        SequencingExperimentES_GenomicFileId(
          sequencing_experiment = EntityConverter.ESequencingExperimentToSequencingExperimentES(sequencingExperiment),
          genomic_file_id = if (sequencingExperimentGenomicFile != null) sequencingExperimentGenomicFile.genomic_file else None
        )
      }
      .groupByKey(_.genomic_file_id)
      .mapGroups((fileId, iterator) => {
        fileId match {
          case Some(id) =>

            val experiments = iterator.map(_.sequencing_experiment).toSeq
            SequencingExperimentsES_GenomicFileId(
              genomic_file_id = id,
              sequencing_experiments = experiments
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
      genomicFile.col("kf_id") === sequencingExperimentsES_GenomicFileId.col("genomic_file_id"),
      "left_outer"
    ).map(tuple => {
      Option(tuple._2) match {
        case Some(_) =>
          EntityConverter.EGenomicFileToGenomicFileES(tuple._1, tuple._2.sequencing_experiments)
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
      observed_phenotype = participant.observed_phenotypes,
      non_observed_phenotype = participant.non_observed_phenotypes,
      race = participant.race,
      study = participant.study
    )
  }
}
