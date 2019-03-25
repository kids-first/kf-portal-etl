package io.kf.etl.processors.participantcentric.transform.steps

import io.kf.etl.es.models.{ParticipantCentric_ES, Participant_ES}
import io.kf.etl.model.utils._
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class BuildParticipantCentric(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[ParticipantCentric_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[ParticipantCentric_ES] = {

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
      .filter(_!=null)

    val files =
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

    val bio_gf =
      ctx.entityDataset.biospecimens
        .joinWith(
          ctx.entityDataset.biospecimenGenomicFiles,
          ctx.entityDataset.biospecimens.col("kfId") === ctx.entityDataset.biospecimenGenomicFiles.col("biospecimenId"),
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
        bio_gf.col("gfId") === files.col("kfId")
      ).map(tuple => {
        BiospecimenId_GenomicFileES(
          bioId = tuple._1.bioId.get,
          file = tuple._2
        )
      })


    val participants_bioId =
      participants.joinWith(
        ctx.entityDataset.biospecimens,
        participants.col("kfId") === ctx.entityDataset.biospecimens.col("participantId"),
        "left_outer"
      ).flatMap(tuple => {

        Option(tuple._2) match {
          case Some(_) => {
            Seq(
              ParticipantES_BiospecimenId(bioId = tuple._2.kfId, participant = tuple._1)
            )
          }
          case None => {
            Seq(
              ParticipantES_BiospecimenId(participant = tuple._1, bioId = None)
            )
          }
        }

      })


    participants_bioId.joinWith(
      bioId_File,
      participants_bioId.col("bioId") === bioId_File.col("bioId"),
      "left_outer"
    ).groupByKey(tuple => {
      tuple._1.participant.kfId.get
    }).mapGroups((_, iterator) => {
      val seq = iterator.toSeq

      val participant = seq(0)._1.participant

      ParticipantCentric_ES(
        affectedStatus = participant.affectedStatus,
        aliasGroup = participant.aliasGroup,
        biospecimens = participant.biospecimens,
        diagnoses = participant.diagnoses,
        ethnicity = participant.ethnicity,
        externalId = participant.externalId,
        family = participant.family,
        familyId = participant.familyId,
        files = seq.collect{
          case tuple if(tuple._2 != null) => tuple._2.file
        },
        gender = participant.gender,
        isProband = participant.isProband,
        kfId = participant.kfId,
        outcome = participant.outcome,
        phenotype = participant.phenotype,
        race = participant.race,
        study = participant.study,
        availableDataTypes = participant.availableDataTypes
      )
    })

  }
}
