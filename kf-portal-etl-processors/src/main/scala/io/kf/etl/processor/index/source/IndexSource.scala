package io.kf.etl.processor.index.source

import io.kf.etl.processor.index.context.IndexContext
import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset


class IndexSource(val context: IndexContext) {
  def source(repo: Repository[Doc]): Dataset[Doc] = {
    repo.load()
  }
}
