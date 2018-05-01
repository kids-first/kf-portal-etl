import Dependencies._

name := "kf-portal-etl-common"

libraryDependencies ++= Seq(
  typesafe_config,
  json4s,
  scalatest % "test"
)