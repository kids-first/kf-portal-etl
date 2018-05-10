package io.kf.etl.processors.download

import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, EntityEndpointSet}
import io.kf.etl.processors.common.processor.Processor
import io.kf.etl.processors.download.context.DownloadContext

class DownloadProcessor(context: DownloadContext,
                        source: Option[Array[String]] => Seq[EntityEndpointSet],
                        transform: Seq[EntityEndpointSet] => EntityDataSet,
                        sink: EntityDataSet => EntityDataSet,
                        output: EntityDataSet => EntityDataSet) extends Processor[Option[Array[String]], EntityDataSet]{

  def process(input:Option[Array[String]]):EntityDataSet = {
    source.andThen(transform).andThen(sink).andThen(output)(input)
  }

}