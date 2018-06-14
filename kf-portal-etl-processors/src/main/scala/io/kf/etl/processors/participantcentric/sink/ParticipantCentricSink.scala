package io.kf.etl.processors.participantcentric.sink

import java.net.URL

import io.kf.etl.es.models.ParticipantCentric_ES
import io.kf.etl.processors.common.ops.URLPathOps
import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext
import org.apache.spark.sql.Dataset

class ParticipantCentricSink(val context: ParticipantCentricContext) {
  private lazy val sinkDataPath = context.getProcessorSinkDataPath()

  def sink(data:Dataset[ParticipantCentric_ES]):Unit = {

    URLPathOps.removePathIfExists(new URL(sinkDataPath), context.appContext)

    import io.kf.etl.transform.ScalaPB2Json4s._
    import context.appContext.sparkSession.implicits._
    data.map(_.toJsonString()).write.text(sinkDataPath)
  }
}
