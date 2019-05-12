import Dependencies._

name := "kf-portal-etl-pipeline"

libraryDependencies ++= Seq(
  spark_sql,
  google_guice,
  reflections,
  es_spark,
  elasticsearch,
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

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
  case "META-INF/DISCLAIMER" => MergeStrategy.last
  case "mozilla/public-suffix-list.txt" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last
  case "git.properties" => MergeStrategy.discard
  case "mime.types" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
assemblyJarName in assembly := "kf-portal-etl.jar"