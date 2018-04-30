package io.kf.etl.processors.participantcentric.transform.steps

import io.kf.etl.es.models.{ParticipantCentric_ES, Participant_ES}
import io.kf.etl.model.utils.BiospecimenId_ParticipantES
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
        ctx.entityDataset.genomicFiles.col("sequencingExperimentId") === ctx.entityDataset.sequencingExperiments.col("kfId")
      ).map(tuple => {
        val file = PBEntityConverter.EGenomicFileToFileES(tuple._1)
        file.copy(
          sequencingExperiments = Seq(
            PBEntityConverter.ESequencingExperimentToSequencingExperimentES(tuple._2)
          )
        )
      })


    val participants_bioId =
      participants.joinWith(
        ctx.entityDataset.biospecimens,
        participants.col("kfId") === ctx.entityDataset.biospecimens.col("participantId")
      ).map(tuple => {
        BiospecimenId_ParticipantES(bioId = tuple._2.kfId.get, participant = tuple._1)
      })

    participants_bioId.joinWith(
      files,
      participants_bioId.col("bioId") === files.col("biospecimenId")
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
        files = seq.map(_._2),
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
