package io.kf.etl.processors.index.context

import com.amazonaws.services.s3.AmazonS3
import io.kf.etl.common.conf.ESConfig
import io.kf.etl.processors.common.processor.{ProcessorConfig, ProcessorContext}
import io.kf.etl.processors.index.transform.releasetag.ReleaseTag
import org.apache.hadoop.fs.{FileSystem => HDFS}
import org.apache.spark.sql.SparkSession


class IndexContext(override val sparkSession: SparkSession,
                    override val hdfs: HDFS,
                   override val appRootPath:String,
                   override val config: IndexConfig,
                   override val s3: AmazonS3) extends ProcessorContext


case class IndexConfig(override val name:String, esConfig: ESConfig, override val dataPath:Option[String], aliasActionEnabled: Boolean, aliasHandlerClass:String, releaseTag: ReleaseTag) extends ProcessorConfig