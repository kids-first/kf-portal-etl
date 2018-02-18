package io.kf.etl.processor.common

import io.kf.etl.common.conf.PostgresqlConfig
import org.apache.hadoop.fs.{FileSystem => HDFSFileSystem}
import org.apache.spark.sql.SparkSession

trait ProcessorContext {
  def hdfs: HDFSFileSystem
  def sparkSession: SparkSession
  def appRootPath: String
  def config: ProcessorConfig
}
