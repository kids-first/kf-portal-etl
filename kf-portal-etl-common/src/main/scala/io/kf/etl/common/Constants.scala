package io.kf.etl.common

object Constants {
  val CONFIG_FILE_URL = "kf.etl.config"
  val DEFAULT_CONFIG_FILE_NAME = "kf_etl.conf"
  val DEFAULT_APP_NAME = "Kids-First-ETL"
  val ROOT_PACKAGE = "io.kf.etl"
  val PROCESSOR_PACKAGE = "io.kf.etl.processor"

  val CONFIG_NAME_SPARK_APP_NAME = s"${ROOT_PACKAGE}.spark.app.name"
  val CONFIG_NAME_SPARK_MASTER = s"${ROOT_PACKAGE}.spark.master"
  val CONFIG_NAME_ES_HOST = s"${ROOT_PACKAGE}.elasticsearch.host"
  val CONFIG_NAME_ES_CLUSTER_NAME = s"${ROOT_PACKAGE}.elasticsearch.cluster_name"
  val CONFIG_NAME_ES_HTTP_PORT = s"${ROOT_PACKAGE}.elasticsearch.http_port"
  val CONFIG_NAME_ES_TRANSPORT_PORT = s"${ROOT_PACKAGE}.elasticsearch.transport_port"
  val CONFIG_NAME_ES_CONFIGS = s"${ROOT_PACKAGE}.elasticsearch.configs"
  val CONFIG_NAME_HDFS_FS = s"${ROOT_PACKAGE}.hdfs.defaultFS"
  val CONFIG_NAME_HDFS_PATH = s"${ROOT_PACKAGE}.hdfs.root"
  val CONFIG_NAME_PROCESSORS = s"${ROOT_PACKAGE}.processors"
  val CONFIG_NAME_PIPELINE = s"${ROOT_PACKAGE}.pipeline"
  val CONFIG_NAME_POSTGRESQL_HOST = s"${ROOT_PACKAGE}.postgresql.host"
  val cONFIG_NAME_POSTGRESQL_DATABASE = s"${ROOT_PACKAGE}.postgresql.database"
  val CONFIG_NAME_POSTGRESQL_USER = s"${ROOT_PACKAGE}.postgresql.user"
  val CONFIG_NAME_POSTGRESQL_PASSWORD = s"${ROOT_PACKAGE}.postgresql.password"
  val CONFIG_NAME_DATA_PATH = "data_path"
  val CONFIG_NAME_WRITE_INTERMEDIATE_DATA = "write_intermediate_data"
  val CONFIG_NAME_HPO = s"${ROOT_PACKAGE}.hpo"
  val HPO_REF_DATA = "hpo_ref"
  val HPO_GRAPH_PATH = "graph_path"

  val FILE_CENTRIC_PROCESSOR_NAME = "file_centric"
  val PARTICIPANT_CENTRIC_PROCESSOR_NAME = "participant_centric"
  val RELEASE_TAG = "release_tag"
  val RELEASE_TAG_CLASS_NAME = "release_tag_class_name"

  val DOWNLOAD_DEFAULT_DATA_PATH = "download"
  val FILECENTRIC_DEFAULT_DATA_PATH = "filecentric"
  val PARTICIPANTCENTRIC_DEFAULT_DATA_PATH = "participantcentric"
  val INDEX_DEFAULT_DATA_PATH = "index"

  val DATASOURCE_OPTION_PROCESSOR_NAME = "kf.etl.processor.name"
  val SPARK_DATASOURCE_OPTION_PATH = "path"
  val HDFS_DATASOURCE_SHORT_NAME = "kf-hdfs"
  val RAW_DATASOURCE_SHORT_NAME = "kf-raw"

  val PROCESSOR_DOCUMENT = "document"
  val PROCESSOR_INDEX = "index"

}
