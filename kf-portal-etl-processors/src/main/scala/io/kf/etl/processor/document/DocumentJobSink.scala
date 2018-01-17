package io.kf.etl.processor.document

import io.kf.model.Doc
import org.apache.spark.sql.Dataset

class DocumentJobSink {
  def sink(data:Dataset[String]):Unit = {
    val context: DocumentJobContext = ???

    data.write.parquet(context.root_path.toString + context.getRelativePath())
  }
}
