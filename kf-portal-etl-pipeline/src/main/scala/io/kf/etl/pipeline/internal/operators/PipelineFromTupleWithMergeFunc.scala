package io.kf.etl.pipeline.internal.operators

import io.kf.etl.pipeline.Pipeline

class PipelineFromTupleWithMergeFunc[T1, T2, T3](s1:T1, s2:T2, merge_func: (T1, T2) => T3) extends Pipeline[T3] {
  override def run(): T3 = {
    merge_func(s1, s2)
  }
}
