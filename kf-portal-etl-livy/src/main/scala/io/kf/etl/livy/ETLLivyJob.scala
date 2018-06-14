package io.kf.etl.livy

import com.google.inject.{AbstractModule, Guice, Injector}
import io.kf.etl.common.Constants.PROCESSOR_PACKAGE
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.context.{CLIParametersHolder, Context, ContextForLivy}
import io.kf.etl.pipeline.Pipeline
import io.kf.etl.processors.download.DownloadProcessor
import io.kf.etl.processors.filecentric.FileCentricProcessor
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.participantcentric.ParticipantCentricProcessor
import io.kf.etl.processors.participantcommon.ParticipantCommonProcessor
import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.SparkSession
import org.reflections.Reflections

import scala.collection.convert.WrapAsScala


class ETLLivyJob(args: Array[String]) extends Job[Unit]{

  private lazy val cliArgs = getCLIArgs()

  private def getCLIArgs(): CLIParametersHolder = {
    new CLIParametersHolder(args)
  }

  override def call(jc: JobContext): Unit = {

    val context = new ContextForLivy(jc.sparkSession[SparkSession]())

    def createInjector(): Injector = {

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
    val injector = createInjector()

    val download = injector.getInstance(classOf[DownloadProcessor])
    val participantcommon = injector.getInstance(classOf[ParticipantCommonProcessor])
    val filecentric = injector.getInstance(classOf[FileCentricProcessor])
    val participantcentric = injector.getInstance(classOf[ParticipantCentricProcessor])
    val index = injector.getInstance(classOf[IndexProcessor])

    Pipeline.from(cliArgs.study_ids).map(download).map(participantcommon).combine(filecentric, participantcentric).map(tuples => {
      Seq(tuples._1, tuples._2).map(tuple => {
        index.process(tuple)
      })
    }).run()
  }
}
