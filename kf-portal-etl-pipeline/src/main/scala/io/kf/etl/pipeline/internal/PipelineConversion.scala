package io.kf.etl.pipeline.internal

import io.kf.etl.processors.common.processor.Processor

object PipelineConversion {
  implicit def function1ToProcessor[I, O](func: I => O): Processor[I, O] = (input: I) => func(input)
}
