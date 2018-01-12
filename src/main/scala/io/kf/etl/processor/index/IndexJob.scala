package io.kf.etl.processor.index

import io.kf.etl.processor.Repository
import io.kf.etl.processor.download.HDFSRepository
import org.apache.spark.sql.Dataset

class IndexJob(source: => Repository, transform: Repository => Dataset[_], sink: Dataset[_] => Unit) {
  def process():Unit = {

    val context: IndexJobContext = ???
    sink(
      transform(source)
    )
  }
}
