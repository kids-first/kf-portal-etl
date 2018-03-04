package io.kf.etl.processors.download.dump

import java.io.{File, FileOutputStream, OutputStream}
import java.net.URL
import java.sql.DriverManager

import io.kf.etl.processors.common.ProcessorCommonDefinitions.PostgresqlDBTables
import io.kf.etl.processors.common.exceptions.KfExceptions.DataDumpTargetNotSupportedException
import io.kf.etl.processors.download.context.DownloadContext
import org.apache.hadoop.fs.Path
import org.postgresql.PGConnection

object PostgresqlDump {
  def dump(ctx: DownloadContext): Unit = {
    val url_fullpath = new URL(s"${ctx.config.dumpPath}")
    val postgresql = ctx.config.postgresql
    val conn = DriverManager.getConnection("jdbc:postgresql://" + postgresql.host + "/" + postgresql.database, postgresql.user, postgresql.password)
    val copyManager = conn.asInstanceOf[PGConnection].getCopyAPI
    PostgresqlDBTables.values.foreach(table => {
      val outputStream = getOutputStreamFromURL(ctx, url_fullpath, table.toString)
      copyManager.copyOut(s"COPY ${table.toString} TO STDOUT (DELIMITER '\t', NULL 'null')", outputStream)
      outputStream.flush()
      outputStream.close()
    })
  }

  private def getOutputStreamFromURL(ctx:DownloadContext, url:URL, tablename:String): OutputStream = {
    url.getProtocol match {
      case "hdfs" => {
        val target = new Path(s"${url.toString}/${tablename}")
        ctx.hdfs.delete(target, true)
        ctx.hdfs.create(target)
      }
      case "file" => {
        val target = new File(s"${url.getFile}/${tablename}")
        target.delete()
        target.createNewFile()
        new FileOutputStream(target)
      }
      case value => throw DataDumpTargetNotSupportedException(url)
    }
  }
}
