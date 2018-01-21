package io.kf.etl.processor.document

import java.net.URL

import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset

class DocumentJob (source: => Repository, transform: Repository => Dataset[String], sink: Dataset[String] => Unit) {

  def process():Repository = {
    val context: DocumentJobContext = ???

    transform.andThen(sink)(source)

    Repository(new URL(context.root_path.toString + "/document"))

  }

}
