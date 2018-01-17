package io.kf.etl.processor.download

import java.net.URL
import java.sql.DriverManager

import io.kf.etl.conf.PostgresqlConfig
import io.kf.etl.processor.repo.Repository
import org.apache.hadoop.fs.{FileSystem, Path}
import org.postgresql.PGConnection

class DataSourceDump(val fs:FileSystem, val root_path:URL, val postgresql: PostgresqlConfig) {

  private val tables = List("")

  def dump():Repository = {

    val fullpath = s"${root_path.toString}/dump"

    val target = new Path(fullpath)

    val conn = DriverManager.getConnection("jdbc:postgresql://" + postgresql.host + "/" + postgresql.database, postgresql.user, postgresql.password)
    val copyManager = conn.asInstanceOf[PGConnection].getCopyAPI
    tables.foreach(table => {
      val outputStream = fs.create(target)
      copyManager.copyOut(s"COPY ${table} TO STDOUT (DELIMITER '\t')", outputStream)
      outputStream.flush()
      outputStream.close()
    })

    Repository(new URL(fullpath))

  }
}
