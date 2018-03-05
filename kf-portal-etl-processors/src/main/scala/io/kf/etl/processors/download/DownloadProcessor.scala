package io.kf.etl.processors.download

import io.kf.etl.processors.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processors.common.processor.Processor
import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.processors.repo.Repository

class DownloadProcessor(context: DownloadContext,
                        source: Unit => Repository,
                        transform: Repository => DatasetsFromDBTables,
                        sink: DatasetsFromDBTables => Unit,
                        output: Unit => Repository) extends Processor[Unit, Repository]{

  def process(input:Unit):Repository = {
    source.andThen(transform).andThen(sink).andThen(output)()
  }

}