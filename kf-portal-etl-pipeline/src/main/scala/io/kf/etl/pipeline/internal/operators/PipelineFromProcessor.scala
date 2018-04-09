package io.kf.etl.pipeline.internal.operators

import io.kf.etl.pipeline.Pipeline

class PipelineFromProcessor[T](val p: Function1[Unit, T]) extends Pipeline[T]{
  override def run(): T = {
    p(Unit)
  }
}
