package io.kf.etl.processor.stage

import io.kf.etl.processor.Repository
import io.kf.model.Project
import org.apache.spark.sql.Dataset

class StageTransformer(val context:StageContext) {
  def transform(repo: Repository): Dataset[Project] = {
    ???
  }
}
