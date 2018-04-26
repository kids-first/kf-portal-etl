package io.kf.etl.processors.filecentric_new.output

import java.net.URL

import io.kf.etl.processors.filecentric_new.context.FileCentricContext
import io.kf.etl.processors.repo.Repository

class FileCentricOutput(val context: FileCentricContext) {
  def output(placeholder:Unit):(String,Repository) = {
    (context.config.name, Repository(new URL(context.getProcessorSinkDataPath())))
  }
}
