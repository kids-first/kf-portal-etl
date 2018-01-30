package io.kf.etl.processor.index

import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset


class IndexJobSource(val context: IndexJobContext) {
  def source(repo: Repository[Doc]): Dataset[Doc] = {
    repo.load()
  }
}
