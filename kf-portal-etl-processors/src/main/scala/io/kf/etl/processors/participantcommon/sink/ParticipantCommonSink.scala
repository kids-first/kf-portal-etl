package io.kf.etl.processors.participantcommon.sink

import java.io.File
import java.net.URL

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.exceptions.KfExceptions.DataSinkTargetNotSupportedException
import io.kf.etl.processors.participantcommon.context.ParticipantCommonContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class ParticipantCommonSink(val context: ParticipantCommonContext) {
  private lazy val sinkDataPath = context.getProcessorSinkDataPath()

  def sink(tuple: (EntityDataSet, Dataset[Participant_ES])): (EntityDataSet, Dataset[Participant_ES]) = {
    checkSinkDirectory(new URL(sinkDataPath))
    import io.kf.etl.transform.ScalaPB2Json4s._
    import context.sparkSession.implicits._

    tuple._2.map(_.toJsonString()).write.text(sinkDataPath)

    tuple
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
