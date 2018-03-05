package io.kf.etl.processors.participantcentric.output

import java.net.URL

import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext
import io.kf.etl.processors.repo.Repository

class ParticipantCentricOutput(val context: ParticipantCentricContext) {
  def output(placeholder:Unit):(String,Repository) = {
    (context.config.name, Repository(new URL(context.getProcessorSinkDataPath())))
  }
}
