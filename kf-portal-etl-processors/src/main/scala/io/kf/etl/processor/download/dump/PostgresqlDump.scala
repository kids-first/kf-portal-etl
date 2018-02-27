package io.kf.etl.processor.download.dump

import java.sql.DriverManager

import io.kf.etl.processor.common.ProcessorCommonDefinitions.DBTables
import io.kf.etl.processor.download.context.DownloadContext
import org.apache.hadoop.fs.Path
import org.postgresql.PGConnection

private[dump] object PostgresqlDump {
  def dump(ctx: DownloadContext): Unit = {
    val fullpath = s"${ctx.config.dumpPath}"

    val postgresql = ctx.config.postgresql
    val fs = ctx.hdfs

    val conn = DriverManager.getConnection("jdbc:postgresql://" + postgresql.host + "/" + postgresql.database, postgresql.user, postgresql.password)
    val copyManager = conn.asInstanceOf[PGConnection].getCopyAPI
    DBTables.values.foreach(table => {
      val target = new Path(s"${fullpath}/${table.toString}")
      val outputStream = fs.create(target)
      copyManager.copyOut(s"COPY ${table.toString} TO STDOUT (DELIMITER '\t', NULL 'null')", outputStream)
      outputStream.flush()
      outputStream.close()
    })
  }
}
