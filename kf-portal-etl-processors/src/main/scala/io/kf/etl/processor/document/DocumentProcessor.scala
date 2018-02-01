package io.kf.etl.processor.document

import java.net.URL

import io.kf.etl.processor.common.Processor
import io.kf.etl.processor.document.context.DocumentContext
import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset

class DocumentProcessor(context: DocumentContext,
                        source: Repository[Doc] => Dataset[Doc],
                        transform: Dataset[Doc] => Dataset[Doc],
                        sink: Dataset[Doc] => Unit,
                        output: Unit => Repository[Doc]) extends Processor[Repository[Doc], Repository[Doc]]{

  def process(input: Repository[Doc]):Repository[Doc] = {
    source.andThen(transform).andThen(sink).andThen(output)(input)
  }

}
