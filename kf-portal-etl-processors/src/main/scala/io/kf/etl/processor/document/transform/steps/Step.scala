package io.kf.etl.processor.document.transform.steps

import io.kf.etl.model.Participant
import io.kf.etl.processor.document.context.DocumentContext
import org.apache.spark.sql.Dataset

case class Step[I, O](val description: String, val handler: StepExecutable[I, O], val posthandler: StepExecutable[O, O] = null) extends Function1[I, O]{
  override def apply(input: I): O = {
    Option(posthandler) match {
      case Some(_) => handler.andThen(posthandler)(input)
      case None => handler(input)
    }
  }
}

abstract class StepExecutable[I, O] extends Function1[I, O]{
  override def apply(v1: I): O = {
    process(v1)
  }

  def process(input: I):O
  def ctx():StepContext
}

class DefaultPostHandler(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]] {
  override def process(input: Dataset[Participant]): Dataset[Participant] = {
    val cached = input.cache()
    cached.write.parquet("")
    cached
  }
}