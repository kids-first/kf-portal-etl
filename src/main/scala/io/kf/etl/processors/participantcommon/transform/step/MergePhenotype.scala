package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.EPhenotype
import io.kf.etl.models.es.{Participant_ES, PhenotypeWithParents_ES, Phenotype_ES}
import io.kf.etl.models.ontology.{HPOOntologyTerm, OntologyTerm}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import org.apache.spark.sql.functions.{collect_list, explode_outer}
import org.apache.spark.sql.{Dataset, SparkSession}

object MergePhenotype {

  type PhenotypeWParentsAtAge = Seq[((String, Seq[String]), Int)]

  def transformPhenotypes(entityDataset: EntityDataSet)(implicit spark: SparkSession): Dataset[(String, Phenotype_ES, PhenotypeWParentsAtAge)] = {
    import entityDataset.phenotypes
    import entityDataset.ontologyData.hpoTerms
    import spark.implicits._

    val phenotype_hpo = phenotypes
      .filter(p =>
        p.observed.getOrElse("").trim.equalsIgnoreCase("positive") ||
          p.observed.getOrElse("").trim.equalsIgnoreCase("negative")) // remove phenotypes the are neither observed nor non-observed
      .joinWith(hpoTerms, phenotypes("hpoIdPhenotype") === hpoTerms("id"), "left_outer")
      .as[(EPhenotype, HPOOntologyTerm)]

    val phenotype_hpo_ancestor = phenotype_hpo.map{
      case (ePhenotype, hpoTerm) if hpoTerm != null => (ePhenotype, hpoTerm, hpoTerm.ancestors)
      case (ePhenotype, hpoTerm) => (ePhenotype, hpoTerm, Nil)
    }
      .withColumnRenamed("_1", "phenotype")
      .withColumnRenamed("_2", "ontological_term")
      .withColumnRenamed("_3", "ancestors")
      .withColumn("ancestor", explode_outer($"ancestors"))
      .drop("ancestors")
      .as[(EPhenotype, HPOOntologyTerm, OntologyTerm)]

    phenotype_hpo_ancestor.show(false)

    val phenotype_hpo_ancestor_parents = phenotype_hpo_ancestor.map{
      case(phenotype, hpoTerm, ontoTerm) => (
        phenotype,
        hpoTerm,
        if(ontoTerm != null && phenotype.ageAtEventDays.isDefined) {(
          (ontoTerm.toString,
            if(ontoTerm != null) ontoTerm.parents else Nil),
          phenotype.ageAtEventDays.get
        )} else null
      )}
      .withColumnRenamed("_1", "phenotype")
      .withColumnRenamed("_2", "ontological_term")
      .withColumnRenamed("_3", "ancestors_with_parents")
      .groupBy("phenotype", "ontological_term")
      .agg(collect_list("ancestors_with_parents"))
      .as[(EPhenotype, HPOOntologyTerm, Seq[((String, Seq[String]), Int)])]

    phenotype_hpo_ancestor_parents.flatMap {
      case(phenotype, hpoTerm, phenotypeWParentsAtAge) if phenotype.participantId.isDefined => {
        val observed = phenotype.observed.map(_.toLowerCase)

        val (hpoObserved, hpoNotObserved) = observed match {
          case Some("positive") if hpoTerm != null => (Option(hpoTerm.toString), None)
          case Some("negative") if hpoTerm != null => (None, Option(hpoTerm.toString))
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
          hpo_phenotype_observed_is_leaf = if(hpoObserved.isDefined) hpoTerm.is_leaf else None,
          hpo_phenotype_not_observed_is_leaf = if(hpoNotObserved.isDefined) hpoTerm.is_leaf else None,
          hpo_phenotype_observed_text = hpoObserved,
          hpo_phenotype_not_observed = hpoNotObserved,
          snomed_phenotype_observed = snomedObserved,
          snomed_phenotype_not_observed = snomedNotObserved,
          source_text_phenotype = sourceText,
          observed = observedOpt
        )
        Some((phenotype.participantId.get, p, phenotypeWParentsAtAge))
      }
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
    ).map(u => (u._1, if(u._2 != null) (u._2._2, u._2._3) else (null, Nil) ))
      .groupByKey { case (participant, _) => participant.kf_id.get }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq: Seq[(Phenotype_ES, (PhenotypeWParentsAtAge, Option[Boolean]))] = groups.filter(_._2._1 != null).map(b => b._2._1 -> (b._2._2, b._2._1.observed))
        participant.copy(
          phenotype = filteredSeq.map(_._1),
          non_observed_phenotypes = filteredSeq
            .filter(_._2._2.contains(false))
            .flatMap(_._2._1)
            .groupBy(_._1)
            .mapValues(_.map(_._2))
            .map(u => PhenotypeWithParents_ES(name = u._1._1, parents = u._1._2, age_at_event_days = u._2.distinct.sorted))
            .toSeq,
          observed_phenotypes = filteredSeq
            .filter(_._2._2.contains(true))
            .flatMap(_._2._1)
            .groupBy(_._1)
            .mapValues(_.map(_._2))
            .map(u => PhenotypeWithParents_ES(name = u._1._1, parents = u._1._2, age_at_event_days = u._2.distinct.sorted))
            .toSeq
        )
      })
  }
}
