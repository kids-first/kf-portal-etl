package io.kf.hpo.processors.download.transform

import io.kf.hpo.models.ontology.OntologyTerm
import org.apache.spark.sql._


case class OntologyTermOutput (
                                id: String,
                                name: String,
                                parents: Seq[BasicOntologyTermOutput] = Nil,
                                ancestors: Seq[BasicOntologyTermOutput] = Nil,
                                is_leaf: Boolean
                              ) {}

case class BasicOntologyTermOutput (
                                   id: String,
                                   name: String
                                   ){}


object WriteJson {
  val spark = SparkSession.builder
    .master("local")
    .appName("HPO")
    .getOrCreate()

  import spark.implicits._

  def toJson(data: Map[OntologyTerm, (Set[OntologyTerm], Boolean)]) = {
    data.map{ case(k, v) => OntologyTermOutput(k.id, k.name, k.parents.map(i => BasicOntologyTermOutput(i.id, i.name)), v._1.map(i => BasicOntologyTermOutput(i.id, i.name)).toSeq, v._2)}.toSeq.toDF().write.mode("overwrite").json("/home/adrianpaul/projects/TEST")
  }
}

