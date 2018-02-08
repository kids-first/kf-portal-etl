package io.kf.etl.processor.document.transform

import io.kf.model.Doc
import org.apache.spark.sql.{Dataset, SparkSession}

class DocumentTransformer(val spark:SparkSession) {

  def transform(input: Dataset[Doc]): Dataset[Doc] = {
    input
  }

}
