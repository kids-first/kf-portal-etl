package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Biospecimen_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EBiospecimen
import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.impl.MergeBiospecimenPerParticipant.formatTerm
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

/**
  * merge all of the biospecimens per participant
  *
  * @param ctx
  */
class MergeBiospecimenPerParticipant(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.entityDataset.biospecimens
    import ctx.entityDataset.ontologyData.ncitTerms
    import ctx.spark.implicits._

    val biospecimenJoinedWithNCIT: Dataset[EBiospecimen] = biospecimens
      .joinWith(ncitTerms, biospecimens.col("ncitIdAnatomicalSite") === ncitTerms.col("id"), "left")
      .map {
        case (biospeciem, term) if term != null => biospeciem.copy(ncitIdAnatomicalSite = formatTerm(term))
        case (biospecimen, _) => biospecimen
      }
      .joinWith(ncitTerms, $"ncitIdTissueType" === ncitTerms.col("id"), "left")
      .map {
        case (biospeciem, term) if term != null => biospeciem.copy(ncitIdTissueType = formatTerm(term))
        case (biospeciem, _) => biospeciem
      }

    participants.joinWith(
      biospecimenJoinedWithNCIT,
      participants.col("kfId") === biospecimenJoinedWithNCIT.col("participantId"),
      "left_outer"
    ).groupByKey { case (participant, _) => participant.kfId }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq: Seq[Biospecimen_ES] = groups.collect { case (_, b) if b != null => PBEntityConverter.EBiospecimenToBiospecimenES(b) }
        participant.copy(
          biospecimens = filteredSeq
        )
      })
  }
}

object MergeBiospecimenPerParticipant {
  def formatTerm(term: OntologyTerm) = Some(s"${term.name} (${term.id})")

}
