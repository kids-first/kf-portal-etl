package io.kf.etl.pipeline.internal.operators

import io.kf.etl.pipeline.Pipeline
import io.kf.etl.processors.common.processor.Processor

class PipelineMapForProcessor[T, O](val source:Pipeline[T], p: Processor[T, O]) extends Pipeline[O]{
  override def run(): O = {
    p(source.run())
  }
}
