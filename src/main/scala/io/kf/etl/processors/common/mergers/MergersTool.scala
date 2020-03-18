package io.kf.etl.processors.common.mergers

import io.kf.etl.models.es.{ObservableAtAge, OntologicalTermWithParents_ES}
import io.kf.etl.models.ontology.{OntologyTerm, OntologyTermBasic}
import org.apache.spark.sql.functions.{collect_list, explode_outer}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.runtime.universe.TypeTag

object MergersTool {

  def mapOntologyTermsToObservable[T <: ObservableAtAge: Encoder : TypeTag]
  (observableDS: Dataset[T], pivotColName: String)
  (ontologyTerms: Dataset[OntologyTerm])
  (implicit spark: SparkSession): Dataset[(T, OntologyTerm, Seq[OntologicalTermWithParents_ES] )] = {

    import spark.implicits._

    val observable_mondo_ancestor = observableDS
      .joinWith(ontologyTerms, observableDS(pivotColName) === ontologyTerms("id"), "left_outer")
      .map {
        case(eObservable, ontologyTerm) if ontologyTerm != null =>
        (eObservable, ontologyTerm, ontologyTerm.ancestors)
        case(eObservable, _) =>
        (eObservable, null, Nil)
      }
      .withColumnRenamed("_1", "eObservable")
      .withColumnRenamed("_2", "ontological_term")
      .withColumnRenamed("_3", "ancestors")
      .withColumn("ancestor", explode_outer($"ancestors"))
      .drop("ancestors")
      .as[(T, OntologyTerm, OntologyTermBasic)]

    observable_mondo_ancestor.map{
      case(observable, mondoTerm, ontoTerm) => (
        observable,
        mondoTerm,
        if(ontoTerm != null && observable.ageAtEventDays.isDefined) {
          OntologicalTermWithParents_ES(
            name = ontoTerm.toString,
            parents = if(ontoTerm != null) ontoTerm.parents else Nil,
            age_at_event_days = Set(observable.ageAtEventDays.get)
          )
        } else null
      )}
      .withColumnRenamed("_1", "observable")
      .withColumnRenamed("_2", "ontological_term")
      .withColumnRenamed("_3", "ancestors_with_parents")
      .groupBy("observable", "ontological_term")
      .agg(collect_list("ancestors_with_parents"))
      .as[(T, OntologyTerm, Seq[OntologicalTermWithParents_ES])]
  }

  def groupPhenotypesWParents (phenotypesWP: Seq[OntologicalTermWithParents_ES]): Seq[OntologicalTermWithParents_ES] =
    phenotypesWP
      .groupBy(_.name)
      .mapValues(p =>
        OntologicalTermWithParents_ES(
          name = p.head.name,
          parents = p.head.parents,
          isLeaf = p.head.isLeaf,
          age_at_event_days = p.flatMap(_.age_at_event_days).toSet))
      .values.toSeq
}
