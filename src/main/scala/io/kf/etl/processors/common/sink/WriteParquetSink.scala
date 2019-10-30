package io.kf.etl.processors.common.sink

import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, SaveMode}

object WriteParquetSink {

  def apply[T](processor:String, data: Dataset[T])(implicit config: Config): Unit = {
    val sinkPath = config.getString(s"processors.$processor.data_path")
    data.write.mode(SaveMode.Overwrite).parquet(sinkPath)

  }
}
