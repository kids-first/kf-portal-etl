package io.kf.etl.processors.index.context

import io.kf.etl.common.conf.ESConfig
import io.kf.etl.processors.common.processor.{ProcessorConfig, ProcessorContext}
import org.apache.hadoop.fs.{FileSystem => HDFS}
import org.apache.spark.sql.SparkSession


class IndexContext(override val sparkSession: SparkSession,
                    override val hdfs: HDFS,
                   override val appRootPath:String,
                   override val config: IndexConfig) extends ProcessorContext


case class IndexConfig(override val name:String, eSConfig: ESConfig, override val dataPath:Option[String]) extends ProcessorConfig