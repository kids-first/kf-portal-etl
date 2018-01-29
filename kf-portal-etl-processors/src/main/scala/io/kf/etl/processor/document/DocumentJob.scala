package io.kf.etl.processor.document

import java.net.URL

import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset

class DocumentJob (source: Repository => Dataset[Doc], transform: Dataset[Doc] => Dataset[Doc], sink: Dataset[Doc] => Unit) {

  def process():Repository = {
    val context: DocumentJobContext = ???

    source.andThen(transform).andThen(sink)

    Repository(new URL(context.getJobDataPath()))

  }

}
