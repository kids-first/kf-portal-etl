package io.kf.etl.context

import java.net.URL

import com.google.inject._
import com.typesafe.config.ConfigFactory
import io.kf.etl.Constants._
import io.kf.etl.conf.{ESConfig, KFConfig, RepoConfig, SparkConfig}
import io.kf.etl.inject.GuiceModule
import org.apache.spark.sql.SparkSession
import org.reflections.Reflections

import scala.collection.convert.WrapAsScala

object Context {
  lazy val injector = createInjector()
  lazy val config = loadConfig()

  private def createInjector():Injector = {

    // load all of the Guice-enabled modules through reflections library
    Guice.createInjector(
      (
        WrapAsScala
          .asScalaSet(
            new Reflections(ROOT_PACKAGE).getTypesAnnotatedWith(classOf[GuiceModule])
          )
          .map(clazz => {
            clazz.newInstance().asInstanceOf[AbstractModule]
          })
          +

        new AbstractModule {
          override def configure(): Unit = ???
          @Provides @Singleton
          def getRepoConfig(): RepoConfig = config.repoConfig
          @Provides @Singleton
          def getSparkConfig(): SparkConfig = config.sparkConfig
          @Provides @Singleton
          def getESConfig(): ESConfig = config.esConfig
          @Provides @Singleton
          def createSparkSession():SparkSession = {
            SparkSession.builder().master(config.sparkConfig.master).appName(config.sparkConfig.appName).getOrCreate()
          }
        }
      ).toSeq:_*
    )

  }

  private def loadConfig(): KFConfig = {

    KFConfig(
      Option( System.getProperty(CONFIG_FILE_URL) ) match {
        case Some(path) => ConfigFactory.parseURL(new URL(path))
        case None => ConfigFactory.load(DEFAULT_CONFIG_FILE_NAME)
      }
    )
  }
}
