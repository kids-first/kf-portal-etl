package io.kf.etl.pipeline.internal.operators

import io.kf.etl.pipeline.Pipeline

class PipelineCombine[T, A1, A2](val source: Pipeline[T], p1: T => A1, p2: T => A2) extends Pipeline[(A1, A2)] {
  override def run(): (A1, A2) = {
    val input = source.run()
    (p1(input), p2(input))

  }

}
