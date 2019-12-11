package io.kf.etl.processors.participantcentric.transform

import io.kf.etl.models.dataservice.{EBiospecimen, EBiospecimenGenomicFile}
import io.kf.etl.models.es.{BiospecimenCombined_ES, GenomicFile_ES, ParticipantCentric_ES, Participant_ES}
import io.kf.etl.models.internal.{SequencingExperimentsES_GenomicFileId, _}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.converter.EntityConverter
import org.apache.spark.sql.{Dataset, SparkSession}

object ParticipantCentricTransformerNew{

  def apply(entityDataset:EntityDataSet, participants: Dataset[Participant_ES])(implicit spark:SparkSession): Dataset[ParticipantCentric_ES] = {
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
          genomicFile = tuple._2.genomicFile
        )
      })
      .groupByKey(_.genomicFile)
      .mapGroups((fileId, iterator) => {
        fileId match {
          case Some(id) =>

            val experiments = iterator.map(_.sequencingExperiment).toSeq
            SequencingExperimentsES_GenomicFileId(
              sequencingExperiments = experiments,
              genomicFile = id
            )
          case None => null
        }
      })
      .filter(_!=null)

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

    val bio_gf: Dataset[(EBiospecimen, Seq[GenomicFile_ES])] =
      entityDataset.biospecimens
        .joinWith(
          entityDataset.biospecimenGenomicFiles,
          entityDataset.biospecimens.col("kfId") === entityDataset.biospecimenGenomicFiles.col("biospecimenId"),
          "left_outer"
        )
        .joinWith(
          files,
          $"_2.genomicFileId" === files("kfId")
        ).as[((EBiospecimen, EBiospecimenGenomicFile), GenomicFile_ES)]
        .map{ case ((biospecimen, _), file) => (biospecimen, file) }
        .as[(EBiospecimen, GenomicFile_ES)]
        .groupByKey { case (biospecimen, _) => biospecimen.kfId }
        .mapGroups { case (_, groupsIterator) =>
          val groups = groupsIterator.toSeq
          val biospecimen: EBiospecimen = groups.head._1
          val files: Seq[GenomicFile_ES] = groups.map(_._2)
          (biospecimen, files)
        }


    participants.joinWith(
      bio_gf,
      participants.col("kfId") === bio_gf("_1.participantId"),
      "left_outer"
    ).as[(Participant_ES, (EBiospecimen, Seq[GenomicFile_ES]))].groupByKey { case (participant, _) => participant.kf_id }
      .mapGroups( { case (_, groupsIterator) =>
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val bioSpecimenCombined: Seq[BiospecimenCombined_ES] = groups.map { case (_, (biospecimen, gfiles)) => EntityConverter.EBiospecimenToBiospecimenCombinedES(biospecimen).copy(genomic_files = gfiles) }

        ParticipantCentric_ES(
          affected_status = participant.affected_status,
          alias_group = participant.alias_group,
          available_data_types = participant.available_data_types,
          biospecimens = bioSpecimenCombined,
          diagnoses = participant.diagnoses,
          diagnosis_category = participant.diagnosis_category  ,
          ethnicity = participant.ethnicity,
          external_id = participant.external_id,
          family = participant.family,
          family_id = participant.family_id,
          files = groups.flatMap(a => a._2._2).distinct,
          gender = participant.gender,
          is_proband = participant.is_proband,
          kf_id = participant.kf_id,
          outcome = participant.outcome,
          phenotype = participant.phenotype,
          race = participant.race,
          study = participant.study
        )
      })

  }

}