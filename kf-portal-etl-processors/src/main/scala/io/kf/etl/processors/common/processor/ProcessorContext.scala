package io.kf.etl.processors.common.processor

import org.apache.hadoop.fs.{FileSystem => HDFSFileSystem}
import org.apache.spark.sql.SparkSession

trait ProcessorContext {
  def hdfs: HDFSFileSystem
  def sparkSession: SparkSession
  def appRootPath: String
  def config: ProcessorConfig
}
