package io.kf.etl.processor.document

import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset


class DocumentJobSource {
  def source(repo: Repository): Dataset[Doc] = {
    repo.load[Doc]()
  }
}
