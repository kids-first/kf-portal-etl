package io.kf.etl.processor.stage

import org.apache.spark.sql.Dataset

class StageJobSink(val context: StageJobContext) {
  def sink(data:Dataset[_]):Unit = {
    data.write.parquet(context.root_path)
  }
}
