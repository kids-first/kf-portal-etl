package io.kf.etl.processor.index.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.conf.ESConfig
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.processor.common.inject.ProcessorInjectModule
import io.kf.etl.processor.index.IndexProcessor
import io.kf.etl.processor.index.context.IndexContext
import io.kf.etl.processor.index.sink.IndexSink
import io.kf.etl.processor.index.source.IndexSource
import io.kf.etl.processor.index.transform.IndexTransformer
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem => HDFS}

import scala.util.{Failure, Success, Try}

@GuiceModule(name = "index")
class IndexInjectModule(sparkSession: SparkSession,
                        hdfs: HDFS,
                        appRootPath: String,
                        config: Option[Config]) extends ProcessorInjectModule(sparkSession, hdfs, appRootPath, config) {
  type CONTEXT = IndexContext
  type PROCESSOR = IndexProcessor
  type SOURCE = IndexSource
  type SINK = IndexSink
  type TRANSFORMER = IndexTransformer

  private def checkConfig(): Boolean = {
    config.isDefined && (
      Try(config.get.getConfig("elasticsearch")) match {
        case Success(cc) => true
        case Failure(_) => false
      }
    )
  }

  require(checkConfig())

  override def getContext(): IndexContext = {
    new IndexContext(sparkSession, hdfs, appRootPath, config)
  }

  @Provides
  override def getProcessor(): IndexProcessor = {
    val context = getContext()
    val source = getSource(context)
    val sink = getSink(context)
    val transformer = getTransformer(context)

    new IndexProcessor(
      context,
      source.source,
      transformer.transform,
      sink.sink
    )
  }

  override def getSource(context: IndexContext): IndexSource = {
    new IndexSource(context)
  }

  override def getSink(context: IndexContext): IndexSink = {

    val esConfig = config.get.getConfig("elasticsearch")
    new IndexSink(
      sparkSession,
      ESConfig(
        esConfig.getString("url"),
        esConfig.getString("index")
      )
    )
  }

  override def getTransformer(context: IndexContext): IndexTransformer = {
    new IndexTransformer(context)
  }

  override def configure(): Unit = ???
}
