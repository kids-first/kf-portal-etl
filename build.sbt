import Dependencies._
import Resolvers._
import sbt.Keys.version

name := "kf-portal-etl"
scalaVersion := "2.11.12"
organization := "io.kf.etl"
version := "1.0.0"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

//initialize := {
//  val _ = initialize.value
//  val javaVersion = sys.props("java.specification.version")
//  if (javaVersion != "1.8")
//    sys.error("Java 1.8 is required for this project. Found " + javaVersion + " instead")
//}

resolvers ++= Seq(
  clojars,
  maven_local,
  twitter,
  spark_packages,
  artima
)


libraryDependencies ++= Seq(
  spark_sql,
  reflections,
  es_spark,
  elasticsearch,
  typesafe_config,
  json4s,
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.3",
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




