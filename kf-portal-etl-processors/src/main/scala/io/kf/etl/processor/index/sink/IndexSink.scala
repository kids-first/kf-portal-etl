package io.kf.etl.processor.index.sink

import io.kf.etl.common.conf.ESConfig
import io.kf.etl.model.filecentric.FileCentric
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

class IndexSink(val spark:SparkSession, val esConfig: ESConfig) {
  def sink(data:Dataset[FileCentric]):Unit = {
    import org.elasticsearch.spark.sql._
    import io.kf.etl.transform.ScalaPB2Json4s._
    import spark.implicits._
//    data.map(_.toJsonString()).toDF().saveToEs(s"${esConfig.index}/doc")
    EsSpark.saveJsonToEs(data.map(_.toJsonString()).rdd, s"${esConfig.index}/doc")
  }
}
