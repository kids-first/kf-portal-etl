package io.kf.etl.processors.test.integration

import com.google.inject.{AbstractModule, Guice, Injector}
import com.typesafe.config.Config
import io.kf.etl.common.Constants.{CONFIG_NAME_DATA_PATH, PROCESSOR_PACKAGE}
import io.kf.etl.common.conf.{MysqlConfig, PostgresqlConfig}
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.context.Context
import io.kf.etl.processors.filecentric.FileCentricProcessor
import io.kf.etl.processors.download.DownloadProcessor
import io.kf.etl.processors.download.context.{DownloadConfig, DownloadContext, HpoConfig}
import io.kf.etl.processors.index.IndexProcessor
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.reflections.Reflections

import scala.collection.convert.WrapAsScala
import scala.util.{Failure, Success, Try}

object IntegrationPipeline {
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
    val download = new IntegrationDownloadProcessor(
      new DownloadContext(
        Context.sparkSession,
        Context.hdfs,
        Context.rootPath,
        {
          val config = Context.getProcessConfig("download")
          DownloadConfig(
            config.get.getString("name"),
            {
              val postgres = config.get.getConfig("postgresql")
              PostgresqlConfig(
                postgres.getString("host"),
                postgres.getString("database"),
                postgres.getString("user"),
                postgres.getString("password")
              )
            },
            config.get.getString("dump_path"),
            Try(config.get.getString(CONFIG_NAME_DATA_PATH)) match {
              case Success(path) => Some(path)
              case Failure(_) => None
            },
            {
              val pg_hpo = config.get.getConfig("hpo.mysql")
              HpoConfig(
                MysqlConfig(
                  pg_hpo.getString("host"),
                  pg_hpo.getString("database"),
                  pg_hpo.getString("user"),
                  pg_hpo.getString("password")
                )
              )
            }
          )
        }
      )
    )
    val index = injector.getInstance(classOf[IndexProcessor])

    download.process().map(t => index.process(Tuple2("file-centric",t)))

  }
}
