package io.kf.etl.processors.index.sink

import io.kf.etl.common.conf.ESConfig
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

class IndexSink(val spark:SparkSession, val esConfig: ESConfig) {
  def sink(data:(String, Dataset[String])):Unit = {
    EsSpark.saveJsonToEs(data._2.rdd, s"${data._1}-${esConfig.index_version}/${data._1}")
  }
}
