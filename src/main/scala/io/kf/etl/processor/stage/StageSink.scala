package io.kf.etl.processor.stage

import io.kf.model.Project
import org.apache.spark.sql.Dataset

class StageSink(val context: StageContext) {
  def sink(data:Dataset[Project]):Unit = {
    ???
  }
}
