package io.kf.etl.processor.document

import io.kf.etl.processor.common.Processor
import io.kf.etl.processor.document.context.DocumentContext
import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset

class DocumentProcessor(context: DocumentContext,
                        source: Repository => Dataset[Doc],
                        transform: Dataset[Doc] => Dataset[Doc],
                        sink: Dataset[Doc] => Unit,
                        output: Unit => Repository) extends Processor[Repository, Repository]{

  def process(input: Repository):Repository = {
    source.andThen(transform).andThen(sink).andThen(output)(input)
  }

}
