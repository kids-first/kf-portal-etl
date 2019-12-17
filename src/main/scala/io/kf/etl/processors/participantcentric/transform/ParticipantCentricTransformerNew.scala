package io.kf.etl.processors.participantcentric.transform

import io.kf.etl.models.dataservice.{EBiospecimen, EBiospecimenGenomicFile}
import io.kf.etl.models.es.{BiospecimenCombined_ES, Biospecimen_ES, GenomicFile_ES, ParticipantCentric_ES, ParticipantCombined_ES, Participant_ES}
import io.kf.etl.models.internal.{SequencingExperimentsES_GenomicFileId, _}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object ParticipantCentricTransformerNew{

  def apply(entityDataset:EntityDataSet, participants: Dataset[Participant_ES])(implicit spark:SparkSession): Dataset[ParticipantCombined_ES] = {
    import spark.implicits._


    val fileId_experiments: Dataset[SequencingExperimentsES_GenomicFileId] =
      entityDataset.sequencingExperiments
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
                genomicFileId = id,
                sequencingExperiments = experiments
              )
            case None => null
          }
        })
        .filter(_ != null)

    val files: Dataset[GenomicFile_ES] =
      entityDataset.genomicFiles.joinWith(
        fileId_experiments,
        entityDataset.genomicFiles.col("kfId") === fileId_experiments.col("genomicFileId"),
        "left_outer"
      ).map(tuple => {
        Option(tuple._2) match {
          case Some(_) =>
            EntityConverter.EGenomicFileToGenomicFileES(
              tuple._1,
              tuple._2.sequencingExperiments)
          case None => EntityConverter.EGenomicFileToGenomicFileES(tuple._1, Seq.empty)
        }
      }) //FIXME is Genomic files already has a sequencing_Experiments????

    val bio_gf: Dataset[(EBiospecimen, Seq[GenomicFile_ES])] = {
      val bioSpec_GFs: Dataset[(EBiospecimen, GenomicFile_ES)] =
        entityDataset.biospecimens
          .joinWith(
            entityDataset.biospecimenGenomicFiles,
            entityDataset.biospecimens.col("kfId") === entityDataset.biospecimenGenomicFiles.col("biospecimenId"),
            "left_outer"
          )
          .as[(EBiospecimen, EBiospecimenGenomicFile)]
          .toDF("eBiospecimen", "eBiospecimenGenomicFile")
          .joinWith(
            files,
            $"eBiospecimenGenomicFile.genomicFileId" === files("kf_id")
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
}