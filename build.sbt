
import Resolvers._
import sbt.Keys.version

name := "kf-portal-etl"


lazy val commonSettings = Seq(
  organization := "io.kf.etl",
  version := "0.1.0",
  scalaVersion := "2.11.12",
  resolvers ++= Seq(
    clojars,
    maven_local,
    novus,
    twitter,
    spark_packages,
    artima
  ),
  test in assembly := {},

  assemblyMergeStrategy in assembly := {
    case PathList("io", "netty", xs @ _*) => MergeStrategy.last
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "ws", xs @ _*) => MergeStrategy.last
    case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
    case PathList("com", "sun", "research", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", "hadoop", xs @ _*) => MergeStrategy.last
    case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", "spark", "unused", xs @ _*) => MergeStrategy.last
    case PathList("javax", "annotation", xs @ _*) => MergeStrategy.first
      // the following is for shading kf-scalapb-json4s-shade version 0.0.1
    case PathList("com", "trueaccord", xs @ _*) => MergeStrategy.last
    case PathList("fastparse", xs @ _*) => MergeStrategy.last
    case PathList("scalapb", xs @ _*) => MergeStrategy.last
    case PathList("sourcecode", xs @ _*) => MergeStrategy.last
    case PathList("com", "thoughtworks", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", "protobuf", xs @ _*) => MergeStrategy.last
      // end for shading scalapb-json4s
    case "META-INF/io.netty.versions.properties" => MergeStrategy.last
    case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
    case "overview.html" => MergeStrategy.last
    case "git.properties" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

lazy val root = (project in file(".")).aggregate(model, common, processors, pipeline)

lazy val model = (project in file("kf-portal-etl-model")).settings(commonSettings:_*)

lazy val common = (project in file("kf-portal-etl-common")).dependsOn(model).settings(commonSettings:_*)

lazy val processors = (project in file("kf-portal-etl-processors")).dependsOn(common%"test->test;compile->compile").settings(commonSettings:_*)

lazy val pipeline = (project in file("kf-portal-etl-pipeline")).dependsOn(processors).settings(commonSettings:_*)


