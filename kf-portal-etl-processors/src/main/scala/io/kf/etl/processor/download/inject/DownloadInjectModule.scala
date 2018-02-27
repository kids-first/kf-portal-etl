package io.kf.etl.processor.download.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.Constants.CONFIG_NAME_DATA_PATH
import io.kf.etl.common.conf.{MysqlConfig, PostgresqlConfig}
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.processor.common.inject.ProcessorInjectModule
import io.kf.etl.processor.download.DownloadProcessor
import io.kf.etl.processor.download.context.{DownloadConfig, DownloadContext, HpoConfig}
import io.kf.etl.processor.download.output.DownloadOutput
import io.kf.etl.processor.download.sink.DownloadSink
import io.kf.etl.processor.download.source.DownloadSource
import io.kf.etl.processor.download.transform.DownloadTransformer
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem => HDFS}

import scala.util.{Failure, Success, Try}

@GuiceModule(name = "download")
class DownloadInjectModule(sparkSession: SparkSession,
                           hdfs: HDFS,
                           appRootPath: String,
                           config: Option[Config]) extends ProcessorInjectModule(sparkSession, hdfs, appRootPath, config) {
  type CONTEXT = DownloadContext
  type PROCESSOR = DownloadProcessor
  type SOURCE = DownloadSource
  type SINK = DownloadSink
  type TRANSFORMER = DownloadTransformer
  type OUTPUT = DownloadOutput

  override def getContext(): DownloadContext = {

    val cc = DownloadConfig(
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
    new DownloadContext(sparkSession, hdfs, appRootPath, cc)
  }

  @Provides
  override def getProcessor(): DownloadProcessor = {
    val context = getContext()
    val source = getSource(context)
    val sink = getSink(context)
    val transformer = getTransformer(context)
    val output = getOutput(context)

    new DownloadProcessor(
      context,
      source.getRepository,
      transformer.transform,
      sink.sink,
      output.output
    )
  }

  override def getSource(context: DownloadContext): DownloadSource = {
    new DownloadSource(context)
  }

  override def getSink(context: DownloadContext): DownloadSink = {
    new DownloadSink(context)
  }

  override def getTransformer(context: DownloadContext): DownloadTransformer = {
    new DownloadTransformer(context)
  }

  override def configure(): Unit = {}

  override def getOutput(context: DownloadContext): DownloadOutput = {
    new DownloadOutput(context)
  }
}
