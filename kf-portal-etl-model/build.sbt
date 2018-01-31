
name := "kf-portal-etl-model"


PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)