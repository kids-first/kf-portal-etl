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
          case Some("positive") => (formatTerm(hpoTerm).toSeq, Nil)
          case Some("negative") => (Nil, formatTerm(hpoTerm).toSeq)
          case _ => (Nil, Nil)
        }

        val (snomedObserved, snomedNotObserved) = observed match {
          case Some("positive") => (phenotype.snomedIdPhenotype.toSeq, Nil)
          case Some("negative") => (Nil, phenotype.snomedIdPhenotype.toSeq)
          case _ => (Nil, Nil)
        }

        // Only append to source text in the positive case for observed
        val sourceText = if (snomedObserved.nonEmpty || hpoObserved.nonEmpty) phenotype.sourceTextPhenotype.toSeq else Nil


        val p = Phenotype_ES(
          ageAtEventDays = phenotype.ageAtEventDays.toSeq,
          externalId = phenotype.externalId.toSeq,
          hpoPhenotypeObserved = hpoObserved,
          hpoPhenotypeObservedText = hpoObserved,
          hpoPhenotypeNotObserved = hpoNotObserved,
          snomedPhenotypeObserved = snomedObserved,
          snomedPhenotypeNotObserved = snomedNotObserved,
          sourceTextPhenotype = sourceText
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
        participant.copy(phenotype = MergePhenotype.reducePhenotype(filteredSeq))

      })
  }


}

object MergePhenotype {

  def reducePhenotype(phenotypes: Seq[Phenotype_ES]): Option[Phenotype_ES] = {

    def agg[T](l1: Seq[T], l2: Seq[T]) = (l1 ++ l2).distinct

    if (phenotypes.isEmpty)
      None
    else
      Some(
        phenotypes.reduce((p1, p2) =>
          Phenotype_ES(
            ageAtEventDays = agg(p1.ageAtEventDays, p2.ageAtEventDays),
            externalId = agg(p1.externalId, p2.externalId),
            hpoPhenotypeNotObserved = agg(p1.hpoPhenotypeNotObserved, p2.hpoPhenotypeNotObserved),
            hpoPhenotypeObserved = agg(p1.hpoPhenotypeObserved, p2.hpoPhenotypeObserved),
            hpoPhenotypeObservedText = agg(p1.hpoPhenotypeObservedText, p2.hpoPhenotypeObservedText),
            snomedPhenotypeNotObserved = agg(p1.snomedPhenotypeNotObserved, p2.snomedPhenotypeNotObserved),
            snomedPhenotypeObserved = agg(p1.snomedPhenotypeObserved, p2.snomedPhenotypeObserved),
            sourceTextPhenotype = agg(p1.sourceTextPhenotype, p2.sourceTextPhenotype)

          )

        ))


  } //end of collectPhenotype
  def formatTerm(term: Option[OntologyTerm]): Option[String] = term.map(t => s"${t.name} (${t.id})")
}