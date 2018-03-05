package io.kf.etl.processors.common.step.posthandler

import java.net.URL

import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext

final case class DefaultPostHandler[T](override val ctx:StepContext = null) extends StepExecutable[T, T] {
  override def process(input: T): T = {
    input
  }
}

case class StepResultTargetNotSupportedException(url:URL) extends Exception(s"Can't write the downloaded data to ${url.toString}, unsupported protocol '${url.getProtocol}'")
