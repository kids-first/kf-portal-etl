package io.kf.etl.processors.common.processor

import com.amazonaws.services.s3.AmazonS3
import org.apache.hadoop.fs.{FileSystem => HDFSFileSystem}
import org.apache.spark.sql.SparkSession

trait ProcessorContext {
  def hdfs: HDFSFileSystem
  def sparkSession: SparkSession
  def appRootPath: String
  def config: ProcessorConfig
  def s3: AmazonS3
}
