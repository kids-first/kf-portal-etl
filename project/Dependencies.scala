
import sbt._

object Dependencies {

  val spark_version = "2.3.0"
  val spark_sql = "org.apache.spark" %% "spark-sql" % spark_version
  val typesafe_config = "com.typesafe" % "config" % "1.3.2"
//  val parquet_protobuf= "org.apache.parquet" % "parquet-protobuf" % "1.9.0"
  val google_guice = "com.google.inject" % "guice" % "4.1.0"
//  val scalapb_grpc_netty = "io.grpc" % "grpc-netty" % com.trueaccord.scalapb.compiler.Version.grpcJavaVersion
//  val scalapb_runtime_grpc = "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion
//  val graphframes = "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"
  val reflections =  "org.reflections" % "reflections" % "0.9.9"
  val asyncHttp = "org.asynchttpclient" % "async-http-client" % "2.4.5"
  val es_spark = "org.elasticsearch" %% "elasticsearch-spark-20" % "6.1.3"
  val elasticsearch = "org.elasticsearch.client" % "transport" % "6.1.3"
  val hadoop265 = "org.apache.hadoop" % "hadoop-client" % "2.6.5"
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.4"
  val embedded_elasticsearch = "pl.allegro.tech" % "embedded-elasticsearch" % "2.4.2"
  val json4s = "org.json4s" %% "json4s-jackson" % "3.2.11"
  val mysql = "mysql" % "mysql-connector-java" % "5.1.46"
  val shaded_scalapb_json4s = "io.kf.etl" % "kf-scalapb-json4s-shade_2.11" % "0.0.1"
  val aws_java_sdk_s3 = "com.amazonaws" % "aws-java-sdk-s3" % "1.11.333"
  val aws_java_sdk_sts =  "com.amazonaws" % "aws-java-sdk-sts" % "1.11.333"
  val livy_scala = "org.apache.livy" %% "livy-scala-api" % "0.5.0-incubating"
  val livy_java =  "org.apache.livy" % "livy-client-http" % "0.5.0-incubating"


}
