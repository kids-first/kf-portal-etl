package io.kf.etl.processors.download.transform

import java.util.Properties

import io.kf.etl.common.conf.MysqlConfig
import io.kf.etl.external.hpo.GraphPath
import io.kf.etl.processors.download.context.DownloadContext
import org.apache.spark.sql.{Dataset, SparkSession}

object HPOGraphPath {
  def get(ctx: DownloadContext): Dataset[GraphPath] = {

    val mysql = ctx.config.mysql
    val spark = ctx.sparkSession

    import spark.implicits._
    val url = s"jdbc:mysql://${ctx.config.mysql.host}/${ctx.config.mysql.database}?user=${ctx.config.mysql.user}&password=${ctx.config.mysql.password}" +
      ctx.config.mysql.properties.mkString("&", "&", "")
    val properties = new Properties()
    spark.read.jdbc(url, "graph_path", properties).map(row => {
      GraphPath(
        term1 = row.getInt(0),
        term2 = row.getInt(1),
        distance = row.getInt(2)
      )
    }).as[GraphPath]
  }
}
