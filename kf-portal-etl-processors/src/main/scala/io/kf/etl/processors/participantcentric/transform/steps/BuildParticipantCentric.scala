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

    val files =
      ctx.entityDataset.genomicFiles.joinWith(
        ctx.entityDataset.sequencingExperiments,
        ctx.entityDataset.genomicFiles.col("sequencingExperimentId") === ctx.entityDataset.sequencingExperiments.col("kfId"),
        "left_outer"
      ).map(tuple => {
        val file = PBEntityConverter.EGenomicFileToFileES(tuple._1)
        file.copy(
          sequencingExperiments ={

            Option(tuple._2) match {
              case Some(_) => Seq(PBEntityConverter.ESequencingExperimentToSequencingExperimentES(tuple._2))
              case None => Seq.empty
            }
          }
        )
      })

    val bio_gf =
      ctx.entityDataset.biospecimens.joinWith(
        ctx.entityDataset.genomicFiles,
        ctx.entityDataset.genomicFiles.col("biospecimenId") === ctx.entityDataset.biospecimens.col("kfId"),
        "left_outer"
      ).flatMap(tuple => {
        Option(tuple._2) match {
          case Some(_) => {
            Seq(
              BiospecimenId_GenomicFileId(
                bioId = Some(tuple._1.kfId.get),
                gfId = Some(tuple._2.kfId.get)
              )
            )
          }
          case None => {
            Seq(
              BiospecimenId_GenomicFileId(
                bioId = Some(tuple._1.kfId.get),
                gfId = None
              )
            )
          }
        }

      })


    val bioId_File =
      bio_gf.joinWith(
        files,
        bio_gf.col("gfId") === files.col("kfId")
      ).map(tuple => {
        BiospecimenId_FileES(
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
        aliasGroup = participant.aliasGroup,
        biospecimens = participant.biospecimens,
        consentType = participant.consentType,
        createdAt = participant.createdAt,
        diagnoses = participant.diagnoses,
        ethnicity = participant.ethnicity,
        externalId = participant.externalId,
        family = participant.family,
        files = seq.collect{
          case tuple if(tuple._2 != null) => tuple._2.file
        },
        gender = participant.gender,
        isProband = participant.isProband,
        kfId = participant.kfId,
        modifiedAt = participant.modifiedAt,
        outcome = participant.outcome,
        phenotype = participant.phenotype,
        race = participant.race,
        study = participant.study
      )
    })

  }
}
