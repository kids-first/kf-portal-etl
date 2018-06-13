package io.kf.etl.context

import org.apache.spark.sql.SparkSession

class DefaultContext extends ContextCommon {

  override def getSparkSession(): SparkSession = {

    esClient // elasticsearch transport client should be created before SparkSesion

    Some(SparkSession.builder()).map(session => {
      config.sparkConfig.master match {
        case Some(master) => session.master(master)
        case None => session
      }
    }).map(session => {

      Seq(
        config.esConfig.configs,
        config.sparkConfig.properties
      ).foreach(entries => {
        entries.foreach(entry => {
          session.config(
            entry._1.substring(entry._1.indexOf("\"") + 1, entry._1.lastIndexOf("\"")) , // remove starting and ending double quotes
            entry._2)
        })
      })

      session
        .config("es.nodes.wan.only", "true")
        .config("es.nodes", s"${config.esConfig.host}:${config.esConfig.http_port}")
        .getOrCreate()

    }).get

  }
}
