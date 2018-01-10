
import Dependencies._
import Resolvers._

name := "kf-portal-etl"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  spark_sql,
  spark_graphx,
  typesafe_config,
  json4s_jackson,
  parquet_protobuf,
  google_guice,
  scalapb_grpc_netty,
  scalapb_runtime_grpc,
  scalapb_json4s,
  graphframes,
  graphql_java,
  reflections,
  asyncHttp,
  ftp4j,
  es_spark,
  scalaz,
  gson,
  postgres
)

resolvers ++= Seq(
  clojars,
  maven_local,
  novus,
  twitter,
  spark_packages
)

//PB.protocVersion := "-v341"

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

