package io.kf.etl.processor.index

import io.kf.etl.conf.ESConfig
import org.apache.spark.sql.{Dataset, SparkSession}

class IndexJobSink(val spark:SparkSession, val esConfig: ESConfig) {
  def sink(data:Dataset[String]):Unit = {
    import org.elasticsearch.spark.sql._
    data.saveToEs(s"${esConfig.index}/doc")
  }
}
