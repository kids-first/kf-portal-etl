package io.kf.etl.processor.download

import java.net.URL
import java.sql.DriverManager

import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository
import org.apache.hadoop.fs.Path
import org.postgresql.PGConnection
import io.kf.etl.processor.common.ProcessorCommonDefinitions.DBTables

class DataSourceDump(val context: DownloadContext) {

  def dump():Repository = {

    val fullpath = s"${context.config.dumpPath}"


    val postgresql = context.config.postgresql
    val fs = context.hdfs

    val conn = DriverManager.getConnection("jdbc:postgresql://" + postgresql.host + "/" + postgresql.database, postgresql.user, postgresql.password)
    val copyManager = conn.asInstanceOf[PGConnection].getCopyAPI
    DBTables.values.foreach(table => {
      val target = new Path(s"${fullpath}/${table.toString}")
      val outputStream = fs.create(target)
      copyManager.copyOut(s"COPY ${table.toString} TO STDOUT (DELIMITER '\t', NULL 'null')", outputStream)
      outputStream.flush()
      outputStream.close()
    })

    Repository(new URL(fullpath))

  }
}
