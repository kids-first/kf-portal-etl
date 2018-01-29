package io.kf.etl.processor.index

import io.kf.etl.common.conf.ESConfig
import io.kf.model.Doc
import org.apache.spark.sql.{Dataset, SparkSession}

class IndexJobSink(val spark:SparkSession, val esConfig: ESConfig) {
  def sink(data:Dataset[Doc]):Unit = {
    import org.elasticsearch.spark.sql._
    data.saveToEs(s"${esConfig.index}/doc")
  }
}
