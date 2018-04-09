package io.kf.etl

import com.google.inject.{AbstractModule, Guice, Injector}
import com.typesafe.config.Config
import io.kf.etl.common.Constants.PROCESSOR_PACKAGE
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.context.Context
import io.kf.etl.pipeline.Pipeline
import io.kf.etl.processors.download.DownloadProcessor
import io.kf.etl.processors.filecentric.FileCentricProcessor
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.participantcentric.ParticipantCentricProcessor
import org.reflections.Reflections

import scala.collection.convert.WrapAsScala

object ETLMain extends App{

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
            classOf[Option[Config]]
          )
            .newInstance(
              Context.getProcessConfig(guiceModuleName)
            )
            .asInstanceOf[AbstractModule]
        }).toSeq:_*
    )
  }

  val download = injector.getInstance(classOf[DownloadProcessor])
  val filecentric = injector.getInstance(classOf[FileCentricProcessor])
  val participantcentric = injector.getInstance(classOf[ParticipantCentricProcessor])
  val index = injector.getInstance(classOf[IndexProcessor])

  Pipeline.from(download).combine(filecentric, participantcentric).map(tuples => {
    Seq(tuples._1, tuples._2).map(tuple => {
      index.process(tuple)
    })
  }).run()

}
