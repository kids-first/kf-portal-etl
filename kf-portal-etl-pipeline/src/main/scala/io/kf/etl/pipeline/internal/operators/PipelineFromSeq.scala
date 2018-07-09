package io.kf.etl.pipeline.internal.operators

import io.kf.etl.pipeline.Pipeline

class PipelineFromSeq[T](seq: Seq[T]) extends Pipeline[Seq[T]] {
  override def run(): Seq[T] = seq
}
