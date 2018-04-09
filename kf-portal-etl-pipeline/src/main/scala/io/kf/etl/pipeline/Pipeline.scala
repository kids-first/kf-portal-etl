package io.kf.etl.pipeline

import io.kf.etl.pipeline.internal.operators._
import io.kf.etl.processors.common.processor.Processor

trait Pipeline[T] {

  def map[A](p: Function1[T, A]): Pipeline[A] = {
    new PipelineMapForFunction1[T, A](this, p)
  }
  def map[A](p: Processor[T, A]): Pipeline[A] = {
    new PipelineMapForProcessor[T, A](this, p)
  }
  def combine[A1, A2](p1: Processor[T, A1], p2: Processor[T, A2]): Pipeline[(A1, A2)] = {
    new PipelineCombine[T, A1, A2](this, p1, p2)
  }
  def merge[A1, A2, A3](p1: Processor[T, A1], p2: Processor[T, A2], merge_func: (A1, A2)=>A3): Pipeline[A3] = {
    new PipelineMerge[T, A1, A2, A3](this, p1, p2, merge_func)
  }
  def run(): T
}

object Pipeline {
  def from[O](p: Function1[Unit, O]): Pipeline[O] =  {
    new PipelineFromProcessor[O](p)
  }
  def from[T](s:T): Pipeline[T] = {
    new PipelineFrom[T](s)
  }
  def from[T1, T2](s1:T1, s2:T2): Pipeline[(T1, T2)] = {
    new PipelineFromTuple[T1, T2](s1, s2)
  }
  def from[T1, T2, T3](s1:T1, s2:T2, merge_func: (T1, T2) => T3): Pipeline[T3] = {
    new PipelineFromTupleWithMergeFunc[T1, T2, T3](s1, s2, merge_func)
  }
}
