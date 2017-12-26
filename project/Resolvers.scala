import sbt._

object Resolvers{
  val clojars= "clojars" at "http://clojars.org/repo/"
  val maven_local = Resolver.mavenLocal
  val novus = "novus" at "http://repo.novus.com/releases/"
  val twitter = "twitter" at "http://maven.twttr.com/"
  val spark_packages = "spark_packages" at "https://dl.bintray.com/spark-packages/maven/"
}