package io.kf.etl.processor.stage

import io.kf.model.Doc
import org.apache.spark.sql.Dataset

class StageJobSink(val context: StageJobContext) {
  def sink(data:Dataset[Doc]):Unit = {
    data.write.parquet(context.root_path.toString)
  }
}
