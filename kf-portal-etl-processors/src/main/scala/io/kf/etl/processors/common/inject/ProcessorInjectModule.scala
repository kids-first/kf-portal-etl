package io.kf.etl.processors.common.inject

import com.google.inject.AbstractModule
import com.typesafe.config.Config
import io.kf.etl.context.Context

abstract class ProcessorInjectModule(val context: Context, val moduleName:String) extends AbstractModule{
//  lazy val sparkSession = context.sparkSession
//  lazy val hdfs = context.hdfs
//  lazy val appRootPath = context.rootPath
//  lazy val s3 = context.awsS3
  lazy val config: Option[Config] = context.getProcessConfig(moduleName)

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
