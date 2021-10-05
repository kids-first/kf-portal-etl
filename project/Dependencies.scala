
import sbt._

object Dependencies {

  val spark_version = "3.1.2"
  val spark_sql = "org.apache.spark" %% "spark-sql" % spark_version % Provided
  val typesafe_config = "com.typesafe" % "config" % "1.3.2"
  val asyncHttp = "org.asynchttpclient" % "async-http-client" % "2.4.5"
  val es_spark = "org.elasticsearch" %% "elasticsearch-spark-30" % "7.12.0"
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.4"
  val json4s = "org.json4s" %% "json4s-jackson" % "3.2.11"
}
