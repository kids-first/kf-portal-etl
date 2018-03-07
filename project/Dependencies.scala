
import sbt._

object Dependencies {

  val spark_version = "2.2.1"
  val spark_sql = "org.apache.spark" %% "spark-sql" % spark_version
  val spark_graphx =  "org.apache.spark" %% "spark-graphx" % spark_version
  val typesafe_config = "com.typesafe" % "config" % "1.3.2"
  val parquet_protobuf= "org.apache.parquet" % "parquet-protobuf" % "1.9.0"
  val google_guice = "com.google.inject" % "guice" % "4.1.0"
  val scalapb_grpc_netty = "io.grpc" % "grpc-netty" % com.trueaccord.scalapb.compiler.Version.grpcJavaVersion
  val scalapb_runtime_grpc = "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion
  val graphframes = "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"
  val graphql_java= "com.graphql-java" % "graphql-java" % "6.0"
  val reflections =  "org.reflections" % "reflections" % "0.9.9"
  val asyncHttp = "org.asynchttpclient" % "async-http-client" % "2.0.38"
  val ftp4j = "it.sauronsoftware" % "ftp4j" % "1.6"
  val es_spark = "org.elasticsearch" %% "elasticsearch-spark-20" % "6.1.3"
  val elasticsearch = "org.elasticsearch.client" % "transport" % "6.1.3"
  val scalaz = "org.scalaz" %% "scalaz-core" % "7.2.18"
  val gson= "com.google.code.gson" % "gson" % "2.8.2"
  val postgres = "org.postgresql" % "postgresql" % "42.1.4"
  val hadoop265 = "org.apache.hadoop" % "hadoop-client" % "2.6.5"
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.4"
  val embedded_elasticsearch = "pl.allegro.tech" % "embedded-elasticsearch" % "2.4.2"
  val json4s = "org.json4s" %% "json4s-jackson" % "3.2.11"
  val mysql = "mysql" % "mysql-connector-java" % "6.0.6"

}