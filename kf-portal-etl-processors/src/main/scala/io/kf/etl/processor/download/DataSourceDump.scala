package io.kf.etl.processor.download

import java.net.URL
import java.sql.DriverManager

import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository
import org.apache.hadoop.fs.Path
import org.postgresql.PGConnection

class DataSourceDump(val context: DownloadContext) {

  private val tables = List("")

  def dump():Repository = {

    val fullpath = s"${context.config.dumpPath}"


    val postgresql = context.config.postgresql
    val fs = context.hdfs

    val conn = DriverManager.getConnection("jdbc:postgresql://" + postgresql.host + "/" + postgresql.database, postgresql.user, postgresql.password)
    val copyManager = conn.asInstanceOf[PGConnection].getCopyAPI
    tables.foreach(table => {
      val target = new Path(s"${fullpath}/${table}")
      val outputStream = fs.create(target)
      copyManager.copyOut(s"COPY ${table} TO STDOUT (DELIMITER '\t')", outputStream)
      outputStream.flush()
      outputStream.close()
    })

    Repository(new URL(fullpath))

  }
}
