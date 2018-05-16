package io.kf.etl.processors.index.es

import io.kf.etl.processors.index.context.IndexContext

trait IndexProcessorAdvice[T1, T2] {
  def preProcess(context: IndexContext, data:T1):Unit
  def postProcess(context: IndexContext, data:T2):Unit
}
