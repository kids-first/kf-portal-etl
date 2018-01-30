package io.kf.etl.processor.common.job

import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem => HDFSFileSystem}
import org.apache.spark.sql.SparkSession

trait ProcessorContext {
  def hdfs: HDFSFileSystem
  def sparkSession: SparkSession
  def appRootPath: String
  def config: Option[Config]
}
