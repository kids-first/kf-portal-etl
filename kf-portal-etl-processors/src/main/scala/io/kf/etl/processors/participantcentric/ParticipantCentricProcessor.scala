package io.kf.etl.processors.participantcentric

import io.kf.etl.model.ParticipantCentric
import io.kf.etl.processors.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processors.common.processor.Processor
import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext
import io.kf.etl.processors.repo.Repository
import org.apache.spark.sql.Dataset

class ParticipantCentricProcessor(
                                 context: ParticipantCentricContext,
                                 source: Repository => DatasetsFromDBTables,
                                 transform: DatasetsFromDBTables => Dataset[ParticipantCentric],
                                 sink: Dataset[ParticipantCentric] => Unit,
                                 output: Unit => (String,Repository)
                                 ) extends Processor[Repository, (String,Repository)]{
  override def process(input: Repository): (String, Repository) = {
    source.andThen(transform).andThen(sink).andThen(output)(input)
  }
}
