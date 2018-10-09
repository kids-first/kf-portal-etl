package io.kf.etl.processors.download.transform.hpo

import java.util.Properties

import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.download.context.DownloadContext
import org.apache.spark.sql.Dataset

object HPOTerm {

  def get(ctx: DownloadContext): Dataset[OntologyTerm] = {
    val mysql = ctx.config.mysql

    val spark = ctx.appContext.sparkSession

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

    spark.read.jdbc(url, "term", properties).map(row => {
      OntologyTerm(
        name = row.getString(1),
        id = row.getString(6)
      )
    }).as[OntologyTerm]
  }
}
