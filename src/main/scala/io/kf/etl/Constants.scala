package io.kf.etl

object Constants {
  val CONFIG_FILE_URL = "kf.etl.config"
  val DEFAULT_CONFIG_FILE_NAME = "kf_etl"
  val DEFAULT_APP_NAME = "Kids-First-ETL"

  val CONFIG_NAME_SPARK_APP_NAME = "io.kf.etl.spark.app.name"
  val CONFIG_NAME_SPARK_MASTER = "io.kf.etl.spark.master"
  val CONFIG_NAME_ES_URL = "io.kf.etl.elasticsearch.url"
  val CONFIG_NAME_ES_INDEX = "io.kf.etl.elasticsearch.index"
  val CONFIG_NAME_HDFS_DEFAULTFS = "io.kf.etl.hdfs.defaultFS"
  val CONFIG_NAME_PROCESSORS = "io.kf.etl.processors"
  val CONFIG_NAME_PIPELINE = "io.kf.etl.pipeline"
}
