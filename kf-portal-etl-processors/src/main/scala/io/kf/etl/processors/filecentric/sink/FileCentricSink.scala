package io.kf.etl.processors.filecentric.sink

import java.net.URL

import io.kf.etl.es.models.FileCentric_ES
import io.kf.etl.processors.common.ops.URLPathOps
import io.kf.etl.processors.filecentric.context.FileCentricContext
import org.apache.spark.sql.Dataset

class FileCentricSink(val context: FileCentricContext) {
  private lazy val sinkDataPath = context.getProcessorSinkDataPath()

  def sink(data:Dataset[FileCentric_ES]):Unit = {
    implicit val hdfs = context.hdfs
    implicit val s3 = context.s3
    URLPathOps.removePathIfExists(new URL(sinkDataPath))

    import io.kf.etl.transform.ScalaPB2Json4s._
    import context.sparkSession.implicits._
    data.map(_.toJsonString()).write.text(sinkDataPath)
  }
}
