package io.kf.etl.processors.common.inject

import com.google.inject.AbstractModule
import com.typesafe.config.Config
import io.kf.etl.context.Context

abstract class ProcessorInjectModule(val config: Option[Config]) extends AbstractModule{
  lazy val sparkSession = Context.sparkSession
  lazy val hdfs = Context.hdfs
  lazy val appRootPath = Context.rootPath

  type CONTEXT
  type PROCESSOR
  type SOURCE
  type SINK
  type TRANSFORMER
  type OUTPUT

  def getContext(): CONTEXT
  def getProcessor(): PROCESSOR

  def getSource(context: CONTEXT): SOURCE
  def getSink(context: CONTEXT): SINK
  def getTransformer(context: CONTEXT): TRANSFORMER
  def getOutput(context: CONTEXT): OUTPUT

}
