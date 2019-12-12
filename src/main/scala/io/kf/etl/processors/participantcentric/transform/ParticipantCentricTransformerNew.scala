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

    val files =
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

    val bio_gf: Dataset[BiospecimenId_GenomicFileId] =
      entityDataset.biospecimens
        .joinWith(
          entityDataset.biospecimenGenomicFiles,
          entityDataset.biospecimens.col("kfId") === entityDataset.biospecimenGenomicFiles.col("biospecimenId"),
          "left_outer"
        )
        .map(tuple => {
          BiospecimenId_GenomicFileId(
            bioId = tuple._1.kfId,
            gfId = {
              Option(tuple._2) match {
                case Some(_) => tuple._2.genomicFileId
                case None => null
              }
            }
          )
        })


    val bioId_File =
      bio_gf.joinWith(
        files,
        bio_gf.col("gfId") === files.col("kf_id")
      ).map(tuple => {
        BiospecimenId_GenomicFileES(
          bioId = tuple._1.bioId.get,
          file = tuple._2
        )
      })

    val participants_bioId =
      participants.joinWith(
        entityDataset.biospecimens,
        participants.col("kf_id") === entityDataset.biospecimens.col("participantId"),
        "left_outer"
      ).flatMap(tuple => {

        Option(tuple._2) match {
          case Some(_) =>
            Seq(ParticipantES_BiospecimenId(bioId = tuple._2.kfId, participant = tuple._1))
          case None =>
            Seq(ParticipantES_BiospecimenId(participant = tuple._1, bioId = None))
        }

      })

    val participents_bio_gf: Dataset[(ParticipantES_BiospecimenId, BiospecimenId_GenomicFileES)] =
      participants_bioId.joinWith(
        bioId_File,
        participants_bioId.col("bioId") === bioId_File.col("bioId"),
        "left_outer"
      )

    participents_bio_gf.show()

    participants
      .groupByKey{case(participant) => participant.kf_id}
      .mapGroups{ case(_, interator) =>
        val  groups = interator.toSeq
        val participant = groups.head

        ParticipantCentric_ES(
          affected_status = participant.affected_status,
          alias_group = participant.alias_group,
          available_data_types = participant.available_data_types,
          biospecimens = Nil,
          diagnoses = participant.diagnoses,
          diagnosis_category = participant.diagnosis_category  ,
          ethnicity = participant.ethnicity,
          external_id = participant.external_id,
          family = participant.family,
          family_id = participant.family_id,
          files = participents_bio_gf.collect().map(a => a._2.file),
          gender = participant.gender,
          is_proband = participant.is_proband,
          kf_id = participant.kf_id,
          outcome = participant.outcome,
          phenotype = participant.phenotype,
          race = participant.race,
          study = participant.study
        )
      }

//    participants.joinWith(
//      bio_gf,
//      participants.col("kfId") === bio_gf.col("gfId"), // bio_gf("_1.participantId"),
//      "left_outer"
//    ).as[(Participant_ES, (EBiospecimen, Seq[GenomicFile_ES]))].groupByKey { case (participant, _) => participant.kf_id }
//      .mapGroups( { case (_, groupsIterator) =>
//        val groups = groupsIterator.toSeq
//        val participant = groups.head._1
//        val bioSpecimenCombined: Seq[BiospecimenCombined_ES] = groups.map { case (_, (biospecimen, gfiles)) => EntityConverter.EBiospecimenToBiospecimenCombinedES(biospecimen).copy(genomic_files = gfiles) }
//
//        ParticipantCentric_ES(
//          affected_status = participant.affected_status,
//          alias_group = participant.alias_group,
//          available_data_types = participant.available_data_types,
//          biospecimens = bioSpecimenCombined,
//          diagnoses = participant.diagnoses,
//          diagnosis_category = participant.diagnosis_category  ,
//          ethnicity = participant.ethnicity,
//          external_id = participant.external_id,
//          family = participant.family,
//          family_id = participant.family_id,
//          files = groups.flatMap(a => a._2._2).distinct,
//          gender = participant.gender,
//          is_proband = participant.is_proband,
//          kf_id = participant.kf_id,
//          outcome = participant.outcome,
//          phenotype = participant.phenotype,
//          race = participant.race,
//          study = participant.study
//        )
//      })

  }

}