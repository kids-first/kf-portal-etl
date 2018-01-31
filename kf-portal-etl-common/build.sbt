import Dependencies._

name := "kf-portal-etl-common"

libraryDependencies ++= Seq(
  spark_sql.exclude("io.netty", "netty") % Provided,
  hadoop265,
  typesafe_config,
  parquet_protobuf,
  google_guice,
  reflections,
  asyncHttp,
  ftp4j,
  es_spark,
  postgres,
  scalatest_scalactic % "test"
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)
