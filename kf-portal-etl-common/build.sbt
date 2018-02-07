import Dependencies._

name := "kf-portal-etl-common"

libraryDependencies ++= Seq(
  typesafe_config,
  json4s,
  scalatest % "test"
)

//PB.targets in Compile := Seq(
//  scalapb.gen() -> (sourceManaged in Compile).value
//)
//
//PB.targets in Test := Seq(
//  scalapb.gen() -> (sourceManaged in Test).value
//)