package io.kf.etl.pipeline.internal.operators

import io.kf.etl.pipeline.Pipeline
import io.kf.etl.processors.common.processor.Processor

class PipelineFromProcessor[T](val p: Processor[Unit, T]) extends Pipeline[T]{
  override def run(): T = {
    p(Unit)
  }
}
