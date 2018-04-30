package io.kf.etl.processors.filecentric.source

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.filecentric.context.FileCentricContext

class FileCentricSource(val context: FileCentricContext) {
  def source(data: EntityDataSet): EntityDataSet = {
    data
  }
}
