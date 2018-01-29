package io.kf.etl.processor.index

import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem => HDFS}
import org.apache.spark.sql.SparkSession
import io.kf.etl.processor.common.job.JobContext


class IndexJobContext(override val hdfs: HDFS,
                      override val sparkSession: SparkSession,
                      override val appRootPath:String,
                      override val config: Option[Config]) extends JobContext
