package io.kf.etl.pipeline.internal.operators

import io.kf.etl.pipeline.Pipeline

class PipelineForeach[T](seq: Seq[T], f: T => Unit) extends Pipeline[Unit] {
  override def run(): Unit = {
    seq.foreach(s => {
      f(s)
    })
  }
}
