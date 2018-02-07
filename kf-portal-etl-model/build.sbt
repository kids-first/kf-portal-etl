
name := "kf-portal-etl-model"

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

PB.targets in Test := Seq(
  scalapb.gen() -> (sourceManaged in Test).value
)