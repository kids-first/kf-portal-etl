package io.kf.hpo.processors.download.transform

import io.kf.hpo.models.ontology.OntologyTerm
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.json4s._


case class OntologyTermOutput (
                         id: String,
                         name: String,
                         parents: Seq[String],
                         ancestors: Seq[String]
                         ){}

object WriteJson {
  val spark = SparkSession.builder
    .master("local")
    .appName("HPO")
    .getOrCreate()

  import spark.implicits._

  def toJson(data: Map[OntologyTerm, Set[String]]) = {
    data.map{ case(k, v) => OntologyTermOutput(k.id, k.name, k.parents, v.toSeq)}.toSeq.toDF().write.mode("overwrite").json("/home/adrianpaul/projects/TEST")
  }
}
