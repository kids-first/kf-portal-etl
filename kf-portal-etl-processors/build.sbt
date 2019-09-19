import Dependencies._

name := "kf-portal-etl-processors"

libraryDependencies ++= Seq(
  spark_sql,
  google_guice,
  reflections,
  es_spark,
  elasticsearch,
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.3",
  shaded_scalapb_json4s,
  kf_es_model,
  embedded_elasticsearch % "test",
  scalatest % "test"
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-annotation" % "2.6.5"
)

assemblyJarName in assembly := "kf-portal-etl.jar"
