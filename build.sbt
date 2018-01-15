
import Dependencies._
import Resolvers._

name := "kf-portal-etl"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  spark_sql,
  typesafe_config,
  parquet_protobuf,
  google_guice,
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

