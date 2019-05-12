
import Resolvers._
import sbt.Keys.version

name := "kf-portal-etl"
scalaVersion := "2.11.12"

lazy val commonSettings = Seq(
  organization := "io.kf.etl",
  version := "0.1.0",
  scalaVersion := "2.11.12",
  resolvers ++= Seq(
    clojars,
    maven_local,
    twitter,
    spark_packages,
    artima
  )
)

lazy val root = (project in file(".")).aggregate(model, common, processors, pipeline).disablePlugins(AssemblyPlugin)

lazy val model = (project in file("kf-portal-etl-model")).settings(commonSettings:_*).disablePlugins(AssemblyPlugin)

lazy val common = (project in file("kf-portal-etl-common")).dependsOn(model).settings(commonSettings:_*).disablePlugins(AssemblyPlugin)

lazy val processors = (project in file("kf-portal-etl-processors")).dependsOn(common%"test->test;compile->compile").settings(commonSettings:_*).disablePlugins(AssemblyPlugin)

lazy val pipeline = (project in file("kf-portal-etl-pipeline")).dependsOn(processors).settings(commonSettings:_*)



