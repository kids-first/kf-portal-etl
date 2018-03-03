package io.kf.etl.processor.index.sink

import io.kf.etl.common.conf.ESConfig
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

class IndexSink(val spark:SparkSession, val esConfig: ESConfig) {
  def sink(data:(String, Dataset[String])):Unit = {
    EsSpark.saveJsonToEs(data._2.rdd, s"${esConfig.index}/${data._1}")
  }
}
