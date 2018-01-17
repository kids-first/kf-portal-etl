
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
    spark_packages
  )
)

lazy val root = (project in file(".")).aggregate(common, processors)

lazy val common = (project in file("kf-portal-etl-common")).settings(commonSettings:_*)

lazy val processors = (project in file("kf-portal-etl-processors")).dependsOn(common).settings(commonSettings:_*)

//lazy val pipeline = (project in file("kf-portal-etl-pipeline")).dependsOn(processors).settings(commonSettings:_*)

//PB.protocVersion := "-v341"

//PB.targets in Compile := Seq(
//  scalapb.gen() -> (sourceManaged in Compile).value
//)

