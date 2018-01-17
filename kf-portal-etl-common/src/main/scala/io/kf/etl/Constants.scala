package io.kf.etl

object Constants {
  val CONFIG_FILE_URL = "kf.etl.config"
  val DEFAULT_CONFIG_FILE_NAME = "kf_etl"
  val DEFAULT_APP_NAME = "Kids-First-ETL"
  val ROOT_PACKAGE = "io.kf.etl"

  val CONFIG_NAME_SPARK_APP_NAME = s"${ROOT_PACKAGE}.spark.app.name"
  val CONFIG_NAME_SPARK_MASTER = s"${ROOT_PACKAGE}.spark.master"
  val CONFIG_NAME_ES_URL = s"${ROOT_PACKAGE}.elasticsearch.url"
  val CONFIG_NAME_ES_INDEX = s"${ROOT_PACKAGE}.elasticsearch.index"
  val CONFIG_NAME_HDFS_FS = s"${ROOT_PACKAGE}.hdfs.defaultFS"
  val CONFIG_NAME_HDFS_PATH = s"${ROOT_PACKAGE}.hdfs.root_path"
  val CONFIG_NAME_PROCESSORS = s"${ROOT_PACKAGE}.processors"
  val CONFIG_NAME_PIPELINE = s"${ROOT_PACKAGE}.pipeline"
  val CONFIG_NAME_POSTGRESQL_HOST = s"${ROOT_PACKAGE}.postgresql.host"
  val cONFIG_NAME_POSTGRESQL_DATABASE = s"${ROOT_PACKAGE}.postgresql.database"
  val CONFIG_NAME_POSTGRESQL_USER = s"${ROOT_PACKAGE}.postgresql.user"
  val CONFIG_NAME_POSTGRESQL_PASSWORD = s"${ROOT_PACKAGE}.postgresql.password"
  val CONFIG_NAME_RELATIVE_PATH = "relative_path"



  val FILE_NAME_ALIQUOT = "aliquot.json"
  val FILE_NAME_DEMOGRAPHIC = "demographic.json"
  val FILE_NAME_READGROUP = "read_group.json"
  val FILE_NAME_SUBMITTED_ALIGNED_READS = "submitted_aligned_reads.json"
  val FILE_NAME_CASE = "case.json"
  val FILE_NAME_DIAGNOSIS = "diagnosis.json"
  val FILE_NAME_SAMPLE = "sample.json"
  val FILE_NAME_TRIO = "trio.json"

  val STAGE_DEFAULT_RELATIVE_PATH = "/stage"
  val DOCUMENT_DEFAULT_RELATIVE_PATH = "/document"
  val INDEX_DEFAULT_RELATIVE_PATH = "/index"
}
