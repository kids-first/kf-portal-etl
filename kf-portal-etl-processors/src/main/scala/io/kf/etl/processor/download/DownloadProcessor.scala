package io.kf.etl.processor.download

import java.net.URL

import io.kf.etl.processor.common.Processor
import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset

import scala.util.Try

class DownloadProcessor(context: DownloadContext,
                        source: Unit => Repository,
                        transform: Repository => Dataset[Doc],
                        sink: Dataset[Doc] => Unit,
                        output: Unit => Try[Repository]) extends Processor[Unit, Try[Repository]]{

  def process(input:Unit):Try[Repository] = {
    source.andThen(transform).andThen(sink).andThen(output)()
  }

}
