package io.kf.etl.processors.common.mergers

import io.kf.etl.models.es.{ObservableAtAge, OntologicalTermWithParents_ES}
import io.kf.etl.models.ontology.{OntologyTerm, OntologyTermBasic}
import org.apache.spark.sql.functions.{collect_list, explode_outer}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.runtime.universe.TypeTag

object OntologyUtil {

  def mapOntologyTermsToObservable[T <: ObservableAtAge: Encoder : TypeTag]
  (observableDS: Dataset[T], pivotColName: String)
  (ontologyTerms: Dataset[OntologyTerm])
  (implicit spark: SparkSession): Dataset[(T, OntologyTerm, Seq[OntologicalTermWithParents_ES] )] = {

    import spark.implicits._

    val observable_ontology_ancestor = observableDS
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

    observable_ontology_ancestor.map{
      case(observable, ontologyTerm, ontologyTermBasic) => (
        observable,
        ontologyTerm,
        if(ontologyTermBasic != null) {
          OntologicalTermWithParents_ES(
            name = ontologyTermBasic.toString,
            parents = if(ontologyTermBasic != null) ontologyTermBasic.parents else Nil,
            age_at_event_days = observable.age_at_event_days.toSet
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

  def groupOntologyTermsWithParents(phenotypesWP: Seq[OntologicalTermWithParents_ES]): Seq[OntologicalTermWithParents_ES] =
    phenotypesWP
      .groupBy(_.name)
      .mapValues(p =>
        OntologicalTermWithParents_ES(
          name = p.head.name,
          parents = p.head.parents,
          is_leaf = p.head.is_leaf,
          is_tagged = p.head.is_tagged,
          age_at_event_days = p.flatMap(_.age_at_event_days).toSet))
      .values.toSeq
}
