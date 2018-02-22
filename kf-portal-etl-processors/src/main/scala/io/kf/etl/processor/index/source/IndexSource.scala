package io.kf.etl.processor.index.source

import io.kf.etl.processor.index.context.IndexContext
import io.kf.etl.processor.repo.Repository
import io.kf.etl.model.FileCentric
import org.apache.spark.sql.Dataset


class IndexSource(val context: IndexContext) {
  def source(repo: Repository): Dataset[FileCentric] = {
    import io.kf.etl.processor.datasource.KfHdfsParquetData._
    import context.sparkSession.implicits._
    context.sparkSession.read.kfHdfsParquet(repo.url.toString).as[FileCentric]
  }
}
