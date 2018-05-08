package io.kf.etl.processors.participantcentric.source

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext
import org.apache.spark.sql.Dataset

class ParticipantCentricSource(val context: ParticipantCentricContext) {
  def source(data: (EntityDataSet, Dataset[Participant_ES])): (EntityDataSet, Dataset[Participant_ES]) = {
    data
  }
}
