package io.kf.etl.processors.filecentric

import io.kf.etl.es.models.FileCentric_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.processor.Processor
import io.kf.etl.processors.filecentric.context.FileCentricContext
import io.kf.etl.processors.repo.Repository
import org.apache.spark.sql.Dataset

class FileCentricProcessor(
                              context: FileCentricContext,
                              source: EntityDataSet => EntityDataSet,
                              transformer: EntityDataSet => Dataset[FileCentric_ES],
                              sink: Dataset[FileCentric_ES] => Unit,
                              output: Unit => (String, Repository)
                              ) extends Processor[EntityDataSet, (String,Repository)]{
  override def process(input: EntityDataSet): (String, Repository) = {
    source.andThen(transformer).andThen(sink).andThen(output)(input)
  }
}
