package io.kf.etl.processors.participantcommon.sink

import java.net.URL

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.ops.URLPathOps
import io.kf.etl.processors.participantcommon.context.ParticipantCommonContext
import org.apache.spark.sql.Dataset

class ParticipantCommonSink(val context: ParticipantCommonContext) {
  private lazy val sinkDataPath = context.getProcessorSinkDataPath()

  def sink(tuple: (EntityDataSet, Dataset[Participant_ES])): (EntityDataSet, Dataset[Participant_ES]) = {

    URLPathOps.removePathIfExists(new URL(sinkDataPath))
    import io.kf.etl.transform.ScalaPB2Json4s._
    import context.sparkSession.implicits._

    tuple._2.map(_.toJsonString()).write.text(sinkDataPath)

    tuple
  }
}
