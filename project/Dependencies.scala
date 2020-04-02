
import sbt._

object Dependencies {

  val spark_version = "2.3.4"
  val spark_sql = "org.apache.spark" %% "spark-sql" % spark_version % Provided
  val typesafe_config = "com.typesafe" % "config" % "1.3.2"
  val reflections =  "org.reflections" % "reflections" % "0.9.9"
  val asyncHttp = "org.asynchttpclient" % "async-http-client" % "2.4.5"
  val es_spark = "org.elasticsearch" %% "elasticsearch-spark-20" % "6.1.3"
  val elasticsearch = "org.elasticsearch.client" % "transport" % "6.1.3"
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.4"
  val json4s = "org.json4s" %% "json4s-jackson" % "3.2.11"
  val typesafe_akka = "com.typesafe.akka" %% "akka-actor" % "2.5.30"
}
