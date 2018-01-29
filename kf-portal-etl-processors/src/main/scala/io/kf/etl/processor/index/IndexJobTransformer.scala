package io.kf.etl.processor.index

import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset

class IndexJobTransformer(val context: IndexJobContext) {

  def transform(input: Dataset[Doc]): Dataset[Doc] = {
    input
  }

}
