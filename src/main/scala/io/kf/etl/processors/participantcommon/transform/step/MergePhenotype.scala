package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.EPhenotype
import io.kf.etl.models.es.{Participant_ES, Phenotype_ES}
import io.kf.etl.models.ontology.OntologyTerm
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import org.apache.spark.sql.{Dataset, SparkSession}

object MergePhenotype {

  def transformPhenotypes(entityDataset: EntityDataSet)(implicit spark: SparkSession): Dataset[(String, Phenotype_ES)] = {
    import entityDataset.ontologyData.hpoTerms
    import entityDataset.phenotypes
    import spark.implicits._

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
          age_at_event_days = phenotype.ageAtEventDays,
          external_id = phenotype.externalId,
          hpo_phenotype_observed = hpoObserved,
          hpo_phenotype_observed_text = hpoObserved,
          hpo_phenotype_not_observed = hpoNotObserved,
          snomed_phenotype_observed = snomedObserved,
          snomed_phenotype_not_observed = snomedNotObserved,
          source_text_phenotype = sourceText,
          observed = observedOpt
        )
        Some(phenotype.participantId.get -> p)
      case _ => None
      }

  }

  def apply(entityDataset: EntityDataSet, participants: Dataset[Participant_ES])(implicit spark: SparkSession): Dataset[Participant_ES] = {

    import spark.implicits._

    val transformedPhenotypes = transformPhenotypes(entityDataset)
    participants.joinWith(
      transformedPhenotypes,
      participants.col("kf_id") === transformedPhenotypes.col("_1"),
      "left_outer"
    ).as[(Participant_ES, Option[(String, Phenotype_ES)])]
      .map {
        case (participant, Some((_, phenotype))) => (participant, Some(phenotype))
        case (participant, _) => (participant, None)
      }
      .groupByKey { case (participant, _) => participant.kf_id.get }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq: Seq[Phenotype_ES] = groups.flatMap(_._2)
        participant.copy(phenotype = filteredSeq)

      })
  }


  def formatTerm(term: Option[OntologyTerm]): Option[String] = term.map(t => s"${t.name} (${t.id})")
}
