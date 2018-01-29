package io.kf.etl.processor.index

import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset

class IndexJob(source: Repository => Dataset[Doc], transform: Dataset[Doc] => Dataset[Doc], sink: Dataset[Doc] => Unit) {
  def process():Unit = {

    val context: IndexJobContext = ???

//    transform.andThen(sink)(source)

    source.andThen(transform).andThen(sink)

  }
}
