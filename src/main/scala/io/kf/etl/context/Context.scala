package io.kf.etl.context

import java.net.URL

import com.google.inject.{AbstractModule, Guice, Injector}
import com.typesafe.config.ConfigFactory
import io.kf.etl.Constants._
import io.kf.etl.conf.KFConfig
import io.kf.etl.inject.GuiceModule
import org.reflections.Reflections

import scala.collection.convert.WrapAsScala

object Context {
  lazy val injector = createInjector()
  lazy val config = loadConfig()

  private def createInjector():Injector = {

    Guice.createInjector(
      WrapAsScala
        .asScalaSet(
          new Reflections(ROOT_PACKAGE).getTypesAnnotatedWith(classOf[GuiceModule])
        )
        .map(clazz => {
          clazz.newInstance().asInstanceOf[AbstractModule]
        })
        .toSeq:_*
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
