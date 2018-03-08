package io.kf.etl.pipeline

import com.google.inject.{AbstractModule, Guice, Injector}
import com.typesafe.config.Config
import io.kf.etl.common.inject.GuiceModule
import org.reflections.Reflections
import io.kf.etl.common.Constants._
import io.kf.etl.context.Context
import io.kf.etl.processors.filecentric.FileCentricProcessor
import io.kf.etl.processors.download.DownloadProcessor
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.participantcentric.ParticipantCentricProcessor
import io.kf.etl.processors.repo.Repository
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
    val filecentric = injector.getInstance(classOf[FileCentricProcessor])
    val participantcentric = injector.getInstance(classOf[ParticipantCentricProcessor])
    val index = injector.getInstance(classOf[IndexProcessor])


    val dump_location = download.process()

    val fp = filecentric.process _
    fp.andThen(index.process)(dump_location)

    val pp = participantcentric.process _
    pp.andThen(index.process)(dump_location)

//    val dp:Unit => Repository = download.process
//    dp.andThen(filecentric.process).andThen(index.process)()
//    dp.andThen(participantcentric.process).andThen(index.process)()

  }

}
