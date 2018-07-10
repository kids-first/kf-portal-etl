package io.kf.etl

import com.google.inject.{AbstractModule, Guice, Injector}
import io.kf.etl.common.Constants.PROCESSOR_PACKAGE
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.context.{CLIParametersHolder, Context, DefaultContext}
import io.kf.etl.pipeline.Pipeline
import io.kf.etl.processors.download.DownloadProcessor
import io.kf.etl.processors.filecentric.FileCentricProcessor
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.participantcentric.ParticipantCentricProcessor
import io.kf.etl.processors.participantcommon.ParticipantCommonProcessor
import org.reflections.Reflections

import scala.collection.convert.WrapAsScala

object ETLMain extends App{

  private lazy val cliArgs = getCLIArgs()

  private lazy val injector = createInjector()

  private lazy val context = createContext()

  private def createContext():Context = {
    new DefaultContext
  }


  private def createInjector(): Injector = {

    Guice.createInjector(

      WrapAsScala
        .asScalaSet(
          new Reflections(PROCESSOR_PACKAGE).getTypesAnnotatedWith(classOf[GuiceModule])
        )
        .map(clazz => {
          val guiceModuleName = clazz.getAnnotation(classOf[GuiceModule]).name()
          clazz.getConstructor(
            classOf[Context],
            classOf[String]
          )
            .newInstance(
              context,
              guiceModuleName
            )
            .asInstanceOf[AbstractModule]
        }).toSeq:_*
    )
  }

  private def getCLIArgs(): CLIParametersHolder = {
    new CLIParametersHolder(args)
  }

  val download = injector.getInstance(classOf[DownloadProcessor])
  val participantcommon = injector.getInstance(classOf[ParticipantCommonProcessor])
  val filecentric = injector.getInstance(classOf[FileCentricProcessor])
  val participantcentric = injector.getInstance(classOf[ParticipantCentricProcessor])
  val index = injector.getInstance(classOf[IndexProcessor])

  cliArgs.study_ids match {
    case Some(study_ids) => {
      Pipeline.foreach[String](study_ids.toSeq, study => {
        Pipeline.from(Some(Array(study))).map(download).map(participantcommon).combine(filecentric, participantcentric).map(tuples => {
          Seq(tuples._1, tuples._2).map(tuple => {
            index.process(
              (s"${tuple._1}_${study}_${cliArgs.release_id.get}".toLowerCase, tuple._2)
            )
          })
        }).run()
      }).run()
    }
    case None => {
      Pipeline.from(None).map(download).map(participantcommon).combine(filecentric, participantcentric).map(tuples => {
        Seq(tuples._1, tuples._2).map(tuple => {
          index.process(tuple)
        })
      }).run()
    }
  }

}
