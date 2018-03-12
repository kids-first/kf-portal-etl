package io.kf.etl.processors.filecentric.transform.steps.context

import io.kf.etl.processors.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Dataset, SparkSession}

case class StepContext(val spark: SparkSession, val processorName:String, val processorDataPath:String, val hdfs: FileSystem, val dbTables: DatasetsFromDBTables)
