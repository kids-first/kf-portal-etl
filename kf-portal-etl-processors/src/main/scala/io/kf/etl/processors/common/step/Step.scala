package io.kf.etl.processors.common.step

import io.kf.etl.processors.common.step.context.StepContext
import io.kf.etl.processors.common.step.posthandler.DefaultPostHandler

case class Step[I, O](description: String, handler: StepExecutable[I, O], posthandler: StepExecutable[O, O] = DefaultPostHandler[O]()) extends Function1[I, O] {
  override def apply(input: I): O = {
    handler.andThen(posthandler)(input)
  }
}

abstract class StepExecutable[-I, +O] extends (I => O) with Serializable {
  override def apply(v1: I): O = {
    process(v1)
  }

  def process(input: I): O

  def ctx: StepContext
}
