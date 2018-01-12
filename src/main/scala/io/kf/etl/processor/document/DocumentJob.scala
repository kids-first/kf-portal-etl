package io.kf.etl.processor.document

import io.kf.etl.processor.Repository
import io.kf.etl.processor.download.HDFSRepository
import org.apache.spark.sql.Dataset

class DocumentJob (source: => Repository, transform: Repository => Dataset[_], sink: Dataset[_] => Unit) {

  def process():Repository = {
    val context: DocumentJobContext = ???
    sink(
      transform(source)
    )

    new HDFSRepository(context.fs, context.root_path + "/document")

  }

}
