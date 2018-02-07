package io.kf.etl.processor.document.source

import io.kf.etl.processor.document.context.DocumentContext
import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset


class DocumentSource(val context: DocumentContext) {
  def source(repo: Repository): Dataset[Doc] = {
    import io.kf.etl.processor.datasource.KfHdfsParquetData._
    import context.sparkSession.implicits._
    context.sparkSession.read.kfHdfs(repo.url.toString).as[Doc]
  }
}
