package io.kf.etl.processors.index.inject

import com.google.inject.Provides
import com.typesafe.config.Config
import io.kf.etl.common.conf.ESConfig
import io.kf.etl.common.inject.GuiceModule
import io.kf.etl.processors.common.inject.ProcessorInjectModule
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.index.context.{IndexContext, IndexConfig}
import io.kf.etl.processors.index.sink.IndexSink
import io.kf.etl.processors.index.source.IndexSource
import io.kf.etl.processors.index.transform.IndexTransformer
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
  type OUTPUT = Unit

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

    val cc = IndexConfig(
      config.get.getString("name"),
      {
        val esConfig = config.get.getConfig("elasticsearch")
        ESConfig(
          esConfig.getString("url"),
          esConfig.getString("index")
        )
      },
      None
    )

    new IndexContext(sparkSession, hdfs, appRootPath, cc)
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

    new IndexSink(
      sparkSession,
      context.config.eSConfig
    )
  }

  override def getTransformer(context: IndexContext): IndexTransformer = {
    new IndexTransformer(context)
  }

  override def configure(): Unit = {}

  override def getOutput(context: IndexContext): Unit = {
    Unit
  }
}
