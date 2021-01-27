package io.kf.etl.processors.participantcommon.transform.step

import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object WriteJsonSink {

  def apply[T](processorName: String)(step: String)(input: Dataset[T])(implicit config: Config, spark: SparkSession): Dataset[T] = {
    if (!config.getBoolean(s"processors.$processorName.write_intermediate_data")) {
      input
    } else {
      val cached = input.cache()
      val dataPath = config.getString(s"processors.$processorName.data_path")
      val stepDataPath = s"$dataPath/steps/$step"

      cached.write.mode(SaveMode.Overwrite).json(stepDataPath)
      cached
    }
  }

  def apply[T](sinkPath: String)(input: Dataset[T])(implicit spark: SparkSession): Dataset[T] = {
    val cached = input.cache()
    input.write.mode(SaveMode.Overwrite).json(sinkPath)
    cached
  }
}
