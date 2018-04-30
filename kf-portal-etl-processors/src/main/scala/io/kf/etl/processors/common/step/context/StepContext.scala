package io.kf.etl.processors.filecentric.transform.steps.context

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

case class StepContext( spark: SparkSession, processorName:String, processorDataPath:String, hdfs: FileSystem, entityDataset: EntityDataSet)
