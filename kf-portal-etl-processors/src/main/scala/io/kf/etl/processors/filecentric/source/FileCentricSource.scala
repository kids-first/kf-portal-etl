package io.kf.etl.processors.filecentric.source

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.filecentric.context.FileCentricContext
import org.apache.spark.sql.Dataset

class FileCentricSource(val context: FileCentricContext) {
  def source(data: (EntityDataSet, Dataset[Participant_ES])): (EntityDataSet, Dataset[Participant_ES]) = {
    data
  }
}
