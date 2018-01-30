package io.kf.etl.processor.index.transform

import io.kf.etl.processor.index.context.IndexContext
import io.kf.model.Doc
import org.apache.spark.sql.Dataset

class IndexTransformer(val context: IndexContext) {

  def transform(input: Dataset[Doc]): Dataset[Doc] = {
    input
  }

}
