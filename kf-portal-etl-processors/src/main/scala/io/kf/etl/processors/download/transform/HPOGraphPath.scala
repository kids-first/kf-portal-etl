package io.kf.etl.processors.download.transform

import java.sql.DriverManager
import java.util.Properties

import io.kf.etl.external.hpo.GraphPath
import io.kf.etl.processors.download.context.DownloadContext
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object HPOGraphPath {

  def get(ctx: DownloadContext): Dataset[GraphPath] = {
    val mysql = ctx.config.mysql

    val spark = ctx.sparkSession

    import spark.implicits._

    val url = s"jdbc:mysql://${mysql.host}/${mysql.database}"

    val properties = new Properties()
    properties.put("user", mysql.user)
    properties.put("password", mysql.password)
    properties.put("driver", "com.mysql.jdbc.Driver")

    mysql.properties.foreach(property => {
      val posOfEqualSign = property.indexOf("=")

      properties.put(
        property.substring(0, posOfEqualSign),
        property.substring(posOfEqualSign+1)
      )

    })

    spark.read.jdbc(url, "graph_path", properties).map(row => {
      GraphPath(
        term1 = "HP:%07d".format(row.getInt(0)),
        term2 = "HP:%07d".format(row.getInt(1)),
        distance = row.getInt(2)
      )
    }).as[GraphPath]
  }
}
