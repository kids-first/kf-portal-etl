package io.kf.etl.processors.download.dump

import java.sql.DriverManager

import io.kf.etl.processors.download.context.DownloadContext

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import io.kf.etl.common.Constants._

object MysqlDump {

  def dump(ctx:DownloadContext):Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    Try(
      DriverManager.getConnection(
        s"jdbc:mysql://${ctx.config.hpo.mysql.user}:${ctx.config.hpo.mysql.password}@${ctx.config.hpo.mysql.host}/${ctx.config.hpo.mysql.database}"
      )
    ) match {
      case Success(conn) => {
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(s"select * from graph_path;")
        val list = new ListBuffer[String]
        while(rs.next()){
          list.append(s"${rs.getInt(1)}\t${rs.getInt(2)}\t${rs.getInt(3)}")
        }

        import ctx.sparkSession.implicits._
        ctx.sparkSession.createDataset(list).write.csv(s"${ctx.config.dumpPath}/${HPO_GRAPH_PATH}")
      }
      case Failure(_) => {

      }
    }
  }

}
