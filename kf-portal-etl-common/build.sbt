import Dependencies._

name := "kf-portal-etl-common"

libraryDependencies ++= Seq(
  typesafe_config,
  json4s,
  shaded_scalapb_json4s,
  hadoop265,
  s3,
  scalatest % "test"
)