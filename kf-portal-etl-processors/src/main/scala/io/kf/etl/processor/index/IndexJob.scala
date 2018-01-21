package io.kf.etl.processor.index

import java.net.URL

import io.kf.etl.processor.repo.Repository
import org.apache.spark.sql.Dataset

class IndexJob(source: => Repository, transform: Repository => Dataset[String], sink: Dataset[String] => Unit) {
  def process():Unit = {

    val context: IndexJobContext = ???

    transform.andThen(sink)(source)

    Repository(new URL(context.root_path.toString + context.getRelativePath()))

  }
}
