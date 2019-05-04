package io.kf.etl.processors.common.step.context

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import org.apache.spark.sql.SparkSession

case class StepContext( spark: SparkSession, processorName:String, processorDataPath:String, entityDataset: EntityDataSet)
