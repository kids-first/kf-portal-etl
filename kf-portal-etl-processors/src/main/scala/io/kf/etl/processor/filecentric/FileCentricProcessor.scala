package io.kf.etl.processor.filecentric

import io.kf.etl.processor.common.Processor
import io.kf.etl.processor.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processor.filecentric.context.DocumentContext
import io.kf.etl.processor.repo.Repository
import io.kf.etl.model.filecentric.FileCentric
import org.apache.spark.sql.Dataset

class FileCentricProcessor(context: DocumentContext,
                           source: Repository => DatasetsFromDBTables,
                           transform: DatasetsFromDBTables => Dataset[FileCentric],
                           sink: Dataset[FileCentric] => Unit,
                           output: Unit => (String,Repository)) extends Processor[Repository, (String,Repository)]{

  def process(input: Repository):(String,Repository) = {
    source.andThen(transform).andThen(sink).andThen(output)(input)
  }

}