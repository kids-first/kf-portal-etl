package io.kf.etl.processors.download

import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, EntityEndpointSet}
import io.kf.etl.processors.common.processor.Processor
import io.kf.etl.processors.download.context.DownloadContext

class DownloadProcessor(context: DownloadContext,
                        source: Map[String,String] => EntityEndpointSet,
                        transform: EntityEndpointSet => EntityDataSet,
                        sink: EntityDataSet => EntityDataSet,
                        output: EntityDataSet => EntityDataSet) extends Processor[Map[String,String], EntityDataSet]{

  def process(input:Map[String,String]):EntityDataSet = {
    source.andThen(transform).andThen(sink).andThen(output)(input)
  }

}