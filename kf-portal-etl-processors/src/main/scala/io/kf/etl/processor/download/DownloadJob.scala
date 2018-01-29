package io.kf.etl.processor.download

import java.net.URL

import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset

import scala.util.Try

class DownloadJob(source: Unit => Repository, transform: Repository => Dataset[Doc], sink: Dataset[Doc] => Unit) {

  def process():Try[Repository] = {
    val context: DownloadJobContext = ???

//    transform.andThen(sink)(source)

    source.andThen(transform).andThen(sink)

    Try(Repository(new URL(context.getJobDataPath())))
  }
}
