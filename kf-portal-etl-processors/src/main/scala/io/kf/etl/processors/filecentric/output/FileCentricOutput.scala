package io.kf.etl.processors.filecentric.output

import java.net.URL

import io.kf.etl.processors.filecentric.context.FileCentricContext
import io.kf.etl.processors.repo.Repository

class FileCentricOutput(val context: FileCentricContext) {
  def output(placeholder:Unit):(String,Repository) = {
    (context.config.name, Repository(new URL(context.getProcessorSinkDataPath())))
  }
}
