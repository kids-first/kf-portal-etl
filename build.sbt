import Dependencies._
import sbt.Keys.version

name := "kf-portal-etl"
scalaVersion := "2.12.14"
organization := "io.kf.etl"
version := "1.0.0"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

libraryDependencies ++= Seq(
  spark_sql,
  es_spark,
  typesafe_config,
  json4s,
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.3",
  scalatest % "test",
  asyncHttp % "test"
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





