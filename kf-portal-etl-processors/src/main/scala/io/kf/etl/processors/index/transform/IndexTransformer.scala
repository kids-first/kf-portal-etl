package io.kf.etl.processors.index.transform

import io.kf.etl.processors.index.context.IndexContext
import org.apache.spark.sql.Dataset

class IndexTransformer(val context: IndexContext) {

  def transform(input: (String, Dataset[String])): (String, Dataset[String]) = {
    input
  }

}
