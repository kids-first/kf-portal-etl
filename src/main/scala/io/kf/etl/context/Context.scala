package io.kf.etl.context

import java.net.URL

import com.google.inject.{Guice, Injector}
import com.typesafe.config.ConfigFactory
import io.kf.etl.Constants._
import io.kf.etl.conf.KFConfig
import io.kf.etl.inject.SparkInjectModule

object Context {
  lazy val injector = createInjector()
  lazy val config = loadConfig()

  private def createInjector():Injector = {
    /*
    *
    *  here we could use the reflections library(refer to: https://github.com/ronmamo/reflections ) to get all guice modules
    *  before the injector is created and initialize them at runtime
    */
    Guice.createInjector(new SparkInjectModule(config.sparkConfig));
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
