package io.kf.etl.processor.stage

import java.net.URL

import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset

class StageJob(source: => Repository, transform: Repository => Dataset[Doc], sink: Dataset[Doc] => Unit) {

  def process():Unit = {

    val context: StageJobContext = ???

    sink(
      transform(source)
    )

    Repository(new URL(context.root_path.toString + context.getRelativePath()))

  }


}
