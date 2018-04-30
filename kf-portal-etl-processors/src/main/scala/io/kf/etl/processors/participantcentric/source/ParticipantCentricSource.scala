package io.kf.etl.processors.participantcentric.source

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext

class ParticipantCentricSource(val context: ParticipantCentricContext) {
  def source(data: EntityDataSet): EntityDataSet = {
    data
  }
}
