package io.kf.etl.processor.download

import io.kf.etl.processor.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processor.common.Processor
import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository
import io.kf.etl.model.DocType
import org.apache.spark.sql.Dataset

import scala.util.Try

class DownloadProcessor(context: DownloadContext,
                        source: Unit => Repository,
                        transform: Repository => DatasetsFromDBTables,
                        sink: DatasetsFromDBTables => Unit,
                        output: Unit => Repository) extends Processor[Unit, Repository]{

  def process(input:Unit):Repository = {
    source.andThen(transform).andThen(sink).andThen(output)()
  }

}