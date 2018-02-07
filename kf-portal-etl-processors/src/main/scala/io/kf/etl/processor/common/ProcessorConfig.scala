package io.kf.etl.processor.common

trait ProcessorConfig {
  def name():String
  def dataPath(): Option[String]
}
