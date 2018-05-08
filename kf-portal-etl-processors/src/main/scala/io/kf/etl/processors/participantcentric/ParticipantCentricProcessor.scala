package io.kf.etl.processors.participantcentric

import io.kf.etl.es.models.{ParticipantCentric_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.processor.Processor
import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext
import io.kf.etl.processors.repo.Repository
import org.apache.spark.sql.Dataset

class ParticipantCentricProcessor(
                                       context: ParticipantCentricContext,
                                       source: ((EntityDataSet, Dataset[Participant_ES])) => (EntityDataSet, Dataset[Participant_ES]),
                                       transform: ((EntityDataSet, Dataset[Participant_ES])) => Dataset[ParticipantCentric_ES],
                                       sink: Dataset[ParticipantCentric_ES] => Unit,
                                       output: Unit => (String, Repository)
                                     )extends Processor[(EntityDataSet, Dataset[Participant_ES]), (String,Repository)] {
  override def process(input: (EntityDataSet, Dataset[Participant_ES])): (String, Repository) = {
    source.andThen(transform).andThen(sink).andThen(output)(input)
  }
}
