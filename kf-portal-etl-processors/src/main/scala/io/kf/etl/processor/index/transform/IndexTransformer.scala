package io.kf.etl.processor.index.transform

import io.kf.etl.processor.index.context.IndexContext
import org.apache.spark.sql.Dataset

class IndexTransformer(val context: IndexContext) {

  def transform(input: (String, Dataset[String])): (String, Dataset[String]) = {
    input
  }

}
