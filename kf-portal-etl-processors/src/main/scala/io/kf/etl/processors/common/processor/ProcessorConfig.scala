package io.kf.etl.processors.common.processor

trait ProcessorConfig {
  def name():String
  def dataPath(): Option[String]
}
