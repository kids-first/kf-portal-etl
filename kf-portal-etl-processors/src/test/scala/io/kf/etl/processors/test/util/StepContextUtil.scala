package io.kf.etl.processors.test.util

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.SparkSession

object StepContextUtil {
  def buildContext(entityDataset: EntityDataSet, name: String = "Test Context")(implicit spark: SparkSession) = StepContext(
    spark = spark,
    processorName = name,
    processorDataPath = "",
    hdfs = null,
    entityDataset = entityDataset
  )
}
