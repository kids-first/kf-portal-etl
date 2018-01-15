package io.kf.etl.processor.stage

import java.net.URL

import io.kf.etl.processor.repo.Repository
import org.apache.spark.sql.Dataset

class StageJob(source: => Repository, transform: Repository => Dataset[_], sink: Dataset[_] => Unit) {

  def process():Unit = {

    val context: StageJobContext = ???

    sink(
      transform(source)
    )

    Repository(new URL(context.root_path.toString + "/stage"))

  }


}
