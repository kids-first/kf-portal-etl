package io.kf.etl.processor.document.output

import java.net.URL

import io.kf.etl.processor.document.context.DocumentContext
import io.kf.etl.processor.repo.Repository

class DocumentOutput(val context: DocumentContext) {
  def output(placeholder:Unit):Repository = {
    Repository(new URL(context.getProcessorSinkDataPath()))
  }
}
