package io.kf.etl.common

object Constants {
  val CONFIG_FILE_URL = "ETL_CONF_FILE"
  val DEFAULT_APP_NAME = "Kids-First-ETL"

  val CONFIG_NAME_SPARK_APP_NAME = "spark.app.name"
  val CONFIG_NAME_SPARK_MASTER = "spark.master"
  val CONFIG_NAME_SPARK_PROPERTIES = "spark.properties"
  val CONFIG_NAME_ES_HOST = "elasticsearch.host"
  val CONFIG_NAME_ES_NODES_WAN_ONLY = "elasticsearch.nodes.wan.only"
  val CONFIG_NAME_ES_CLUSTER_NAME = "elasticsearch.cluster_name"
  val CONFIG_NAME_ES_PORT = "elasticsearch.port"
  val CONFIG_NAME_ES_SCHEME = "elasticsearch.scheme"
  val CONFIG_NAME_ES_CONFIGS = "elasticsearch.configs"
  val CONFIG_NAME_DATASERVICE_URL = "dataservice.url"
  val CONFIG_NAME_DATASERVICE_LIMIT = "dataservice.limit"
  val CONFIG_NAME_DATASERVICE_DOMAIN_DCF = "dataservice.dcf_host"
  val CONFIG_NAME_DATASERVICE_DOMAIN_GEN3 = "dataservice.gen3_host"
  val CONFIG_NAME_NCI_CRDR_DATA = "processors.download.nci_crdc_path"
  val CONFIG_NAME_PROCESSORS = "processors"
  val CONFIG_NAME_PIPELINE = "pipeline"

  val CONFIG_NAME_DATA_PATH = "data_path"
  val CONFIG_NAME_MONDO_PATH = "processors.download.mondo_path"
  val CONFIG_NAME_NCIT_PATH = "processors.download.ncit_path"
  val CONFIG_NAME_HPO_PATH = "processors.download.hpo_path"
  val CONFIG_NAME_DUOCODE_PATH = "processors.download.duocode_path"
  val CONFIG_NAME_WRITE_INTERMEDIATE_DATA = "write_intermediate_data"
  val DEFAULT_FILE_CENTRIC_ALIAS = "file_centric"
  val DEFAULT_PARTICIPANT_CENTRIC_ALIAS = "participant_centric"
  val FILE_CENTRIC_MAPPING_FILE_NAME = "file_centric.mapping.json"
  val PARTICIPANT_CENTRIC_MAPPING_FILE_NAME = "participant_centric.mapping.json"
  val STUDY_CENTRIC_MAPPING_FILE_NAME = "study_centric.mapping.json"

  val DATA_CAT_AVAILABLE_DATA_TYPES = "processors.download.data_category_existing_data_path"

  val JSON_OUTPUT_FILES = "aws.s3.json_output_files_path"
}
