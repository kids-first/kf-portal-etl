package io.kf.etl.processors.participantcommon.source

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.participantcommon.context.ParticipantCommonContext

class ParticipantCommonSource(val context: ParticipantCommonContext) {
  def source(data: EntityDataSet): EntityDataSet = {
    data
  }
}
