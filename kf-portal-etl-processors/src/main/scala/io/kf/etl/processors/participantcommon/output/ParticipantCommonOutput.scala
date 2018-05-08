package io.kf.etl.processors.participantcommon.output

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.participantcommon.context.ParticipantCommonContext
import org.apache.spark.sql.Dataset

class ParticipantCommonOutput(val context: ParticipantCommonContext) {
  def output(tuple: (EntityDataSet, Dataset[Participant_ES])): (EntityDataSet, Dataset[Participant_ES]) = {
    tuple
  }
}
