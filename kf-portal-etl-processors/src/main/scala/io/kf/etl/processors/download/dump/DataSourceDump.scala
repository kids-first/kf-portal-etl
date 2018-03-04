package io.kf.etl.processors.download.dump

import java.io.File
import java.net.URL

import io.kf.etl.processors.common.exceptions.KfExceptions.{CreateDataSinkDirectoryFailedException, CreateDumpDirectoryFailedException, DataDumpTargetNotSupportedException}
import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.processors.repo.Repository
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

class DataSourceDump(val context: DownloadContext) {

  def dump():Repository = {

    checkTargetPath(new URL(s"${context.config.dumpPath}"))

    PostgresqlDump.dump(context)

    MysqlDump.dump(context)

    Repository(new URL(s"${context.config.dumpPath}"))

  }

  private def checkTargetPath(url:URL):Unit = {
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
      case value => throw DataDumpTargetNotSupportedException(url)
    }
  }
}
