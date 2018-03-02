package io.kf.etl.processor.index.transform

import io.kf.etl.processor.index.context.IndexContext
import io.kf.etl.model.filecentric.FileCentric
import org.apache.spark.sql.Dataset

class IndexTransformer(val context: IndexContext) {

  def transform(input: Dataset[FileCentric]): Dataset[FileCentric] = {
    input
  }

}
