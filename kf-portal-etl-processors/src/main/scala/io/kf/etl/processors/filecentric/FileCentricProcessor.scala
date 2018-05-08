package io.kf.etl.processors.filecentric

import io.kf.etl.es.models.{FileCentric_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.processor.Processor
import io.kf.etl.processors.filecentric.context.FileCentricContext
import io.kf.etl.processors.repo.Repository
import org.apache.spark.sql.Dataset

class FileCentricProcessor(
                            context: FileCentricContext,
                            source: ((EntityDataSet, Dataset[Participant_ES])) => (EntityDataSet, Dataset[Participant_ES]),
                            transformer: ((EntityDataSet, Dataset[Participant_ES])) => Dataset[FileCentric_ES],
                            sink: Dataset[FileCentric_ES] => Unit,
                            output: Unit => (String, Repository)
                              ) extends Processor[(EntityDataSet, Dataset[Participant_ES]), (String,Repository)]{
  override def process(input: (EntityDataSet, Dataset[Participant_ES])): (String, Repository) = {
    source.andThen(transformer).andThen(sink).andThen(output)(input)
  }
}
