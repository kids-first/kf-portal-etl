package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Participant_ES, Phenotype_ES}
import io.kf.etl.external.dataservice.entity.EPhenotype
import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.context.StepContext
import io.kf.etl.processors.common.step.impl.MergePhenotype._
import org.apache.spark.sql.Dataset

class MergePhenotype(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {

  def transformPhenotypes(): Dataset[(String, Phenotype_ES)] = {
    import ctx.entityDataset.ontologyData.hpoTerms
    import ctx.entityDataset.phenotypes
    import ctx.spark.implicits._

    phenotypes
      .joinWith(hpoTerms, phenotypes("hpoIdPhenotype") === hpoTerms("id"), "left_outer")
      .as[(EPhenotype, Option[OntologyTerm])]
      .flatMap { case (phenotype, hpoTerm) if phenotype.participantId.isDefined =>
        val observed = phenotype.observed.map(_.toLowerCase)
        val (hpoObserved, hpoNotObserved) = observed match {
          case Some("positive") => (formatTerm(hpoTerm), None)
          case Some("negative") => (None, formatTerm(hpoTerm))
          case _ => (None, None)
        }

        val (snomedObserved, snomedNotObserved) = observed match {
          case Some("positive") => (phenotype.snomedIdPhenotype, None)
          case Some("negative") => (None, phenotype.snomedIdPhenotype)
          case _ => (None, None)
        }

        val observedOpt = observed match {
          case Some("positive") => Some(true)
          case Some("negative") => Some(false)
          case _ => None
        }

        // Only append to source text in the positive case for observed
        val sourceText = if (snomedObserved.nonEmpty || hpoObserved.nonEmpty) phenotype.sourceTextPhenotype else None


        val p = Phenotype_ES(
          ageAtEventDays = phenotype.ageAtEventDays,
          externalId = phenotype.externalId,
          hpoPhenotypeObserved = hpoObserved,
          hpoPhenotypeObservedText = hpoObserved,
          hpoPhenotypeNotObserved = hpoNotObserved,
          snomedPhenotypeObserved = snomedObserved,
          snomedPhenotypeNotObserved = snomedNotObserved,
          sourceTextPhenotype = sourceText,
          observed = observedOpt
        )
        Some(phenotype.participantId.get -> p)
      case _ => None
      }

  }

  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {

    import ctx.spark.implicits._

    val transformedPhenotypes = transformPhenotypes()
    participants.joinWith(
      transformedPhenotypes,
      participants.col("kfId") === transformedPhenotypes.col("_1"),
      "left_outer"
    ).as[(Participant_ES, Option[(String, Phenotype_ES)])]
      .map {
        case (participant, Some((_, phenotype))) => (participant, Some(phenotype))
        case (participant, _) => (participant, None)
      }
      .groupByKey { case (participant, _) => participant.kfId.get }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq: Seq[Phenotype_ES] = groups.flatMap(_._2)
        participant.copy(phenotype = filteredSeq)

      })
  }


}

object MergePhenotype {
  def formatTerm(term: Option[OntologyTerm]): Option[String] = term.map(t => s"${t.name} (${t.id})")
}