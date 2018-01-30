package io.kf.etl.pipeline

import com.google.inject.{AbstractModule, Guice, Injector}
import com.typesafe.config.Config
import io.kf.etl.common.context.Context
import io.kf.etl.common.inject.GuiceModule
import org.reflections.Reflections
import io.kf.etl.common.Constants._
import io.kf.etl.processor.document.DocumentProcessor
import io.kf.etl.processor.download.DownloadProcessor
import io.kf.etl.processor.index.IndexProcessor
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

import scala.collection.convert.WrapAsScala

object Pipeline {

  private lazy val injector = createInjector()

  private def createInjector(): Injector = {

    Guice.createInjector(

      WrapAsScala
        .asScalaSet(
          new Reflections(PROCESSOR_PACKAGE).getTypesAnnotatedWith(classOf[GuiceModule])
        )
        .map(clazz => {
          val guiceModuleName = clazz.getAnnotation(classOf[GuiceModule]).name()
          clazz.getConstructor(
                classOf[SparkSession],
                classOf[FileSystem],
                classOf[String],
                classOf[Option[Config]])
              .newInstance(
                Context.sparkSession,
                Context.hdfs,
                Context.rootPath,
                Context.getProcessConfig(guiceModuleName)
              ).asInstanceOf[AbstractModule]
        }).toSeq:_*
    )
  }

  def run():Unit = {
    val download = injector.getInstance(classOf[DownloadProcessor])
    val document = injector.getInstance(classOf[DocumentProcessor])
    val index = injector.getInstance(classOf[IndexProcessor])

    download.process().map(index.process(_))

  }

}
