import Dependencies._

name := "kf-portal-etl-processors"

libraryDependencies ++= Seq(
  spark_sql.exclude("io.netty", "netty"),
  hadoop265,
  parquet_protobuf,
  google_guice,
  reflections,
  es_spark,
  elasticsearch,
  postgres,
  mysql,
  embedded_elasticsearch % "test",
  scalatest % "test",
  asyncHttp % "test"
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-annotation" % "2.6.5",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)

assemblyJarName in assembly := "kf-portal-etl.jar"
