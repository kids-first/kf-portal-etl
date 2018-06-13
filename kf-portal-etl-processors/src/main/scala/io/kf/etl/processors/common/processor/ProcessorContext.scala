package io.kf.etl.processors.common.processor

import io.kf.etl.context.Context

trait ProcessorContext {
  def config: ProcessorConfig
  def appContext: Context
}
