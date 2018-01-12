package io.kf.etl.processor.stage

import io.kf.etl.processor.Repository
import io.kf.etl.processor.download.HDFSRepository
import org.apache.spark.sql.Dataset

class StageJob(source: => Repository, transform: Repository => Dataset[_], sink: Dataset[_] => Unit) {

  def process():Unit = {

    val context: StageJobContext = ???

    sink(
      transform(source)
    )
    new HDFSRepository(context.fs, context.root_path + "/stage")

  }


}
