package io.kf.etl.processors.participantcommon

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.processor.Processor
import io.kf.etl.processors.participantcommon.context.ParticipantCommonContext
import org.apache.spark.sql.Dataset

class ParticipantCommonProcessor(
                                context: ParticipantCommonContext,
                                source: EntityDataSet => EntityDataSet,
                                transformer: EntityDataSet => (EntityDataSet, Dataset[Participant_ES]),
                                sink: ((EntityDataSet, Dataset[Participant_ES])) => (EntityDataSet, Dataset[Participant_ES]),
                                output: ((EntityDataSet, Dataset[Participant_ES])) => (EntityDataSet, Dataset[Participant_ES])
                                ) extends Processor[EntityDataSet, (EntityDataSet,Dataset[Participant_ES])]{
  override def process(input: EntityDataSet): (EntityDataSet, Dataset[Participant_ES]) = {
    source.andThen(transformer).andThen(sink).andThen(output)(input)
  }
}
