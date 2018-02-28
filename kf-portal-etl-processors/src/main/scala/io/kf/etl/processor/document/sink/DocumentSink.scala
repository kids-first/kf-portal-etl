package io.kf.etl.processor.document.sink

import java.io.File
import java.net.URL

import io.kf.etl.processor.document.context.DocumentContext
import io.kf.etl.model.FileCentric
import io.kf.etl.processor.common.exceptions.KfExceptions.{CreateDataSinkDirectoryFailedException, DataSinkTargetNotSupportedException}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class DocumentSink(val context: DocumentContext) {
  def sink(data:Dataset[FileCentric]):Unit = {
    checkSinkDirectory(new URL(context.getJobDataPath()))

    data.write.parquet(context.getJobDataPath())
  }

  private def checkSinkDirectory(url: URL):Unit = {
    val url = new URL(context.getJobDataPath())

    url.getProtocol match {
      case "hdfs" => {
        val dir = new Path(url.toString)
        context.hdfs.delete(dir, true)
        context.hdfs.mkdirs(dir)
      }
      case "file" => {
        val dir = new File(url.getFile)
        if(dir.exists())
          FileUtils.deleteDirectory(dir)
        dir.mkdir() match {
          case false => throw CreateDataSinkDirectoryFailedException(url)
          case true =>
        }
      }
      case value => DataSinkTargetNotSupportedException(url)
    }
  }
}
