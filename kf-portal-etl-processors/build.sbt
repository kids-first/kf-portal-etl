import Dependencies._

name := "kf-portal-etl-processors"

libraryDependencies ++= Seq(
  postgres
)

assemblyJarName in assembly := "kf-portal-etl.jar"