import Dependencies._

name := "kf-portal-etl-processors"

libraryDependencies ++= Seq(
  spark_sql.exclude("io.netty", "netty"),
  embedded_elasticsearch % "test",
  scalatest % "test"
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-annotation" % "2.6.5",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)


assemblyJarName in assembly := "kf-portal-etl.jar"

//assemblyShadeRules in assembly := Seq(
//  ShadeRule.rename("com.fasterxml.jackson.**" -> "io.kf.etl.shade.@1").inLibrary("com.fasterxml.jackson.core" % "jackson-databind" % "2.8.4"),
//  ShadeRule.rename("org.json4s.**" -> "io.kf.etl.shade.@1").inLibrary("org.json4s" %% "json4s-jackson" % "3.5.1")
//)