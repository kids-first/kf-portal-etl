package io.kf.etl.pipeline.internal.operators

import io.kf.etl.pipeline.Pipeline

class PipelineFromTuple[T1, T2](s1:T1, s2:T2) extends Pipeline[(T1, T2)]{
  override def run(): (T1, T2) = {
    (s1, s2)
  }
}
