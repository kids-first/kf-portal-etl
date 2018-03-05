package io.kf.etl.processors.participantcentric.sink

import java.io.File
import java.net.URL

import io.kf.etl.model.ParticipantCentric
import io.kf.etl.processors.common.exceptions.KfExceptions.DataSinkTargetNotSupportedException
import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class ParticipantCentricSink(val context: ParticipantCentricContext) {
  private lazy val sinkDataPath = context.getProcessorSinkDataPath()

  def sink(data:Dataset[ParticipantCentric]):Unit = {
    checkSinkDirectory(new URL(sinkDataPath))

    import io.kf.etl.transform.ScalaPB2Json4s._
    import context.sparkSession.implicits._
    data.map(_.toJsonString()).write.text(sinkDataPath)
  }

  private def checkSinkDirectory(url: URL):Unit = {

    url.getProtocol match {
      case "hdfs" => {
        val dir = new Path(url.toString)
        context.hdfs.delete(dir, true)
      }
      case "file" => {
        val dir = new File(url.getFile)
        if(dir.exists())
          FileUtils.deleteDirectory(dir)
      }
      case value => DataSinkTargetNotSupportedException(url)
    }
  }
}
