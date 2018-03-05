package io.kf.etl.processors.filecentric

import io.kf.etl.processors.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processors.filecentric.context.FileCentricContext
import io.kf.etl.processors.repo.Repository
import io.kf.etl.model.FileCentric
import io.kf.etl.processors.common.processor.Processor
import org.apache.spark.sql.Dataset

class FileCentricProcessor(context: FileCentricContext,
                           source: Repository => DatasetsFromDBTables,
                           transform: DatasetsFromDBTables => Dataset[FileCentric],
                           sink: Dataset[FileCentric] => Unit,
                           output: Unit => (String,Repository)) extends Processor[Repository, (String,Repository)]{

  override def process(input: Repository):(String,Repository) = {
    source.andThen(transform).andThen(sink).andThen(output)(input)
  }

}