package io.kf.etl.processors.index.source

import io.kf.etl.processors.index.context.IndexContext
import io.kf.etl.processors.repo.Repository
import org.apache.spark.sql.Dataset


class IndexSource(val context: IndexContext) {
  def source(tuple: (String, Repository)): (String, Dataset[String]) = {
    (tuple._1, context.sparkSession.read.textFile(tuple._2.url.toString))
  }
}
