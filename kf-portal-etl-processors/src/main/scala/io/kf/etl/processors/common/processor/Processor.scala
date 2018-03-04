package io.kf.etl.processors.common.processor

trait Processor[I, O] extends Function1[I, O]{
  override def apply(input: I): O = {
    process(input)
  }
  def process(input:I): O
}


