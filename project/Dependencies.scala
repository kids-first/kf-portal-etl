
import sbt._

object Dependencies {

  val spark_version = "2.2.1"
  val spark_sql = "org.apache.spark" %% "spark-sql" % spark_version
  val spark_graphx =  "org.apache.spark" %% "spark-graphx" % spark_version
  val typesafe_config = "com.typesafe" % "config" % "1.3.2"
  val json4s_jackson =  "org.json4s" %% "json4s-jackson" % "3.5.3"
  val parquet_protobuf= "org.apache.parquet" % "parquet-protobuf" % "1.9.0"
  val google_guice = "com.google.inject" % "guice" % "4.1.0"



  val scala_pb = Seq(
    "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.7",
    "com.trueaccord.scalapb" %% "scalapb-json4s" % "0.3.2",
    "io.grpc" % "grpc-netty" % com.trueaccord.scalapb.compiler.Version.grpcJavaVersion,
    "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion
  )
}