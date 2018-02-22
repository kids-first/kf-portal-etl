package io.kf.etl.processor.document

import io.kf.etl.processor.common.Processor
import io.kf.etl.processor.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processor.document.context.DocumentContext
import io.kf.etl.processor.repo.Repository
import io.kf.etl.model.FileCentric
import org.apache.spark.sql.Dataset

class DocumentProcessor(context: DocumentContext,
                        source: Repository => DatasetsFromDBTables,
                        transform: DatasetsFromDBTables => Dataset[FileCentric],
                        sink: Dataset[FileCentric] => Unit,
                        output: Unit => Repository) extends Processor[Repository, Repository]{

  def process(input: Repository):Repository = {
    source.andThen(transform).andThen(sink).andThen(output)(input)
  }

}