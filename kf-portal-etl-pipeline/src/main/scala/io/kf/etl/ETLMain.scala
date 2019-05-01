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

object ETLMain extends App {

  private lazy val cliArgs = getCLIArgs()

  private lazy val injector = createInjector()

  private lazy val context = createContext()

  private def createContext(): DefaultContext = {
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
        }).toSeq: _*
    )
  }

  private def getCLIArgs(): CLIParametersHolder = {
    new CLIParametersHolder(args)
  }

  private def createIndexName(indexType: String, studyId: String, releaseId: String): String = {
    s"${indexType}_${studyId}_${releaseId}".toLowerCase
  }

  /**
    * *** INJECT DEPENDENCIES ***
    */
  val downloadProcessor = injector.getInstance(classOf[DownloadProcessor])
  val participantCommonProcessor = injector.getInstance(classOf[ParticipantCommonProcessor])
  val fileCentricProcessor = injector.getInstance(classOf[FileCentricProcessor])
  val participantCentricProcessor = injector.getInstance(classOf[ParticipantCentricProcessor])
  val indexProcessor = injector.getInstance(classOf[IndexProcessor])


  /**
    * *** RUN PIPELINE ***
    */
  cliArgs.study_ids match {

    // Requires study_ids to run
    case Some(study_ids) =>
      println(s"Running Pipeline with study IDS {${study_ids.mkString(", ")}}")

      /* REJOICE! THE PIPELINE BEGINS!!! */
      Pipeline.foreach[String](study_ids.toSeq, study => {

        println(s"Beginning pipeline for study: $study")

        Pipeline.from(study)
          .map(downloadProcessor)
          .map(participantCommonProcessor)
          .combine(fileCentricProcessor, participantCentricProcessor)
          .map(tuples => {
            // run the index processor for each completed index.
            // TODO: Rebuild this into the pipeline syntax, should not require logic inside the pipeline

            Seq(tuples._1, tuples._2).foreach(tuple => {
              val indexType = tuple._1
              val releaseId = cliArgs.release_id.get

              val indexName = createIndexName(indexType, study, releaseId)

              indexProcessor.process((indexName, tuple._2))
            })
          })
          .run()
        context.sparkSession.sqlContext.clearCache()

      }).run()

    // No Study IDs:
    case None => {
      println(s"No Study IDs provided - Nothing to run.")
      // TODO: Throw exception
    }

  }
  context.close()
}
