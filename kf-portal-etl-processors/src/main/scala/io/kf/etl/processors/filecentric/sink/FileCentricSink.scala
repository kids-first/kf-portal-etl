package io.kf.etl.processors.filecentric.sink

import java.io.File
import java.net.URL

import io.kf.etl.es.models.FileCentric_ES
import io.kf.etl.processors.common.exceptions.KfExceptions.DataSinkTargetNotSupportedException
import io.kf.etl.processors.filecentric.context.FileCentricContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class FileCentricSink(val context: FileCentricContext) {
  private lazy val sinkDataPath = context.getProcessorSinkDataPath()

  def sink(data:Dataset[FileCentric_ES]):Unit = {
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
