package io.kf.etl.processor.filecentric.output

import java.net.URL

import io.kf.etl.processor.filecentric.context.DocumentContext
import io.kf.etl.processor.repo.Repository

class FileCentricOutput(val context: DocumentContext) {
  def output(placeholder:Unit):(String,Repository) = {
    ("file-centric", Repository(new URL(context.getProcessorSinkDataPath())))
  }
}
