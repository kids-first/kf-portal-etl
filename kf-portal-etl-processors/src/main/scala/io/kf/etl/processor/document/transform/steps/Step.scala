package io.kf.etl.processor.document.transform.steps

import io.kf.etl.model.Participant
import io.kf.etl.processor.document.context.DocumentContext
import org.apache.spark.sql.Dataset

case class Step[I, O](val description: String, val handler: StepExecutable[I, O], val posthandler: StepExecutable[O, O] = DefaultPostHandler[O]()) extends Function1[I, O]{
  override def apply(input: I): O = {
    handler.andThen(posthandler)(input)
  }
}

abstract class StepExecutable[-I, +O] extends Function1[I, O]{
  override def apply(v1: I): O = {
    process(v1)
  }

  def process(input: I):O
  def ctx():StepContext
}

final case class DefaultPostHandler[T](override val ctx:StepContext = null) extends StepExecutable[T, T] {
  override def process(input: T): T = {
    input
  }
}
