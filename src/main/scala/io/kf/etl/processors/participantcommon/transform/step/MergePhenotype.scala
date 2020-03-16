package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.EPhenotype
import io.kf.etl.models.es.{Participant_ES, PhenotypeWithParents_ES, Phenotype_ES}
import io.kf.etl.models.ontology.{OntologyTerm, OntologyTermBasic}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import org.apache.spark.sql.functions.{collect_list, explode_outer}
import org.apache.spark.sql.{Dataset, SparkSession}

object MergePhenotype {

  def transformPhenotypes(entityDataset: EntityDataSet)(implicit spark: SparkSession): Dataset[(String, Phenotype_ES, Seq[PhenotypeWithParents_ES])] = {
    import entityDataset.phenotypes
    import entityDataset.ontologyData.hpoTerms
    import spark.implicits._

    val phenotype_hpo = phenotypes
      .filter { p => p.observed match {
        case Some(o) => Seq("positive", "negative").contains(o.trim.toLowerCase)
        case _ => false
      }}
      .joinWith(hpoTerms, phenotypes("hpoIdPhenotype") === hpoTerms("id"), "left_outer")
      .as[(EPhenotype, OntologyTerm)]

    val phenotype_hpo_ancestor = phenotype_hpo.map{
      case (ePhenotype, hpoTerm) if hpoTerm != null => (ePhenotype, hpoTerm, hpoTerm.ancestors)
      case (ePhenotype, hpoTerm) => (ePhenotype, hpoTerm, Nil)
    }
      .withColumnRenamed("_1", "phenotype")
      .withColumnRenamed("_2", "ontological_term")
      .withColumnRenamed("_3", "ancestors")
      .withColumn("ancestor", explode_outer($"ancestors"))
      .drop("ancestors")
      .as[(EPhenotype, OntologyTerm, OntologyTermBasic)]

    val phenotype_hpo_ancestor_parents = phenotype_hpo_ancestor.map{
      case(phenotype, hpoTerm, ontoTerm) => (
        phenotype,
        hpoTerm,
        if(ontoTerm != null && phenotype.ageAtEventDays.isDefined) {
          PhenotypeWithParents_ES(
            name = ontoTerm.toString,
            parents = if(ontoTerm != null) ontoTerm.parents else Nil,
            age_at_event_days = Seq(phenotype.ageAtEventDays.get)
          )
        } else null
      )}
      .withColumnRenamed("_1", "phenotype")
      .withColumnRenamed("_2", "ontological_term")
      .withColumnRenamed("_3", "ancestors_with_parents")
      .groupBy("phenotype", "ontological_term")
      .agg(collect_list("ancestors_with_parents"))
      .as[(EPhenotype, OntologyTerm, Seq[PhenotypeWithParents_ES])]

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
          hpo_phenotype_observed_text = hpoObserved,
          hpo_phenotype_not_observed = hpoNotObserved,
          snomed_phenotype_observed = snomedObserved,
          snomed_phenotype_not_observed = snomedNotObserved,
          source_text_phenotype = sourceText,
          observed = observedOpt
        )
        Some((
          phenotype.participantId.get,
          p,
          if(hpoTerm != null){
            PhenotypeWithParents_ES(
              name = hpoTerm.toString,
              parents = hpoTerm.parents,
              age_at_event_days = if(phenotype.ageAtEventDays.isDefined) Seq(phenotype.ageAtEventDays.get) else Nil,
              isLeaf = hpoTerm.isLeaf) +: phenotypeWParentsAtAge
          } else Nil
        ))
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
    ).map{
      case (participant_ES, (_, phenotype_ES, seqPhenotypeWParents_ES)) =>
        (participant_ES,  (phenotype_ES, seqPhenotypeWParents_ES) )
      case (participant_ES, _) => (participant_ES, (null, null))
    }
      .groupByKey { case (participant, _) => participant.kf_id.get }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq: Seq[(Phenotype_ES, (Seq[PhenotypeWithParents_ES], Option[Boolean]))] =
          groups.filter(_._2._1 != null).map{ case(_, (phenotype_ES, phenotypeWParents_ES)) =>
            phenotype_ES -> (phenotypeWParents_ES, phenotype_ES.observed)
          }
        participant.copy(
          phenotype = filteredSeq.map(_._1),
          non_observed_phenotypes = groupPhenotypesWParents(
            filteredSeq.filter(_._2._2.contains(false)).flatMap(_._2._1)
          ),
          observed_phenotypes = groupPhenotypesWParents(
            filteredSeq.filter(_._2._2.contains(true)).flatMap(_._2._1)
          )
        )
      })
  }

  private def groupPhenotypesWParents (phenotypesWP: Seq[PhenotypeWithParents_ES]): Seq[PhenotypeWithParents_ES] =
    phenotypesWP
      .groupBy(_.name)
      .mapValues(p =>
        PhenotypeWithParents_ES(
          name = p.head.name,
          parents = p.head.parents,
          isLeaf = p.head.isLeaf,
          age_at_event_days = p.flatMap(_.age_at_event_days).distinct.sorted))
      .values.toSeq.sorted
}
