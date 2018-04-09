package io.kf.etl.pipeline.internal.operators

import io.kf.etl.pipeline.Pipeline

class PipelineMapForFunction1[T, O](val source:Pipeline[T], p: Function1[T, O]) extends Pipeline[O]{
  override def run(): O = {
    p(source.run())
  }
}
