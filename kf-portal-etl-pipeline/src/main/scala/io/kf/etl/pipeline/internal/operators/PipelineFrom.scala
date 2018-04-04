package io.kf.etl.pipeline.internal.operators

import io.kf.etl.pipeline.Pipeline

class PipelineFrom[T](val s: T) extends Pipeline[T]{
  override def run(): T = {
    s
  }
}
