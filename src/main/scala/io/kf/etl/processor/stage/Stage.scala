package io.kf.etl.processor.stage

import io.kf.etl.processor.Repository
import io.kf.model.Project
import org.apache.spark.sql.Dataset

class Stage(source: => Repository, transform: Repository => Dataset[Project], sink: Dataset[Project] => Unit) {

  def process():Unit = {
    sink(
      transform(source)
    )
  }

}
