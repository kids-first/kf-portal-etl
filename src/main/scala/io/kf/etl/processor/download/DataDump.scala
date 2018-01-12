package io.kf.etl.processor.download

import java.sql.DriverManager

import io.kf.etl.conf.PostgresqlConfig
import io.kf.etl.processor.Repository
import org.apache.hadoop.fs.{FileSystem, Path}
import org.postgresql.PGConnection

class DataDump(val fs:FileSystem, val root_path:String, val postgresql: PostgresqlConfig) {

  private val subPath4Dump = "dump"
  private val tables = List("")

  def dump():Repository = {

    val target = new Path(s"${root_path}/${subPath4Dump}")

    val conn = DriverManager.getConnection("jdbc:postgresql://" + postgresql.host + "/" + postgresql.database, postgresql.user, postgresql.password)
    val copyManager = conn.asInstanceOf[PGConnection].getCopyAPI
    tables.foreach(table => {
      val outputStream = fs.create(target)
      copyManager.copyOut(s"COPY ${table} TO STDOUT (DELIMITER '\t')", outputStream)
      outputStream.flush()
      outputStream.close()
    })

    HDFSRepository(fs, root_path + "/" + subPath4Dump)

  }
}
