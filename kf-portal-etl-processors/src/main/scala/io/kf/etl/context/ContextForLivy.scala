package io.kf.etl.context

import org.apache.spark.sql.SparkSession

class ContextForLivy(session: SparkSession) extends ContextCommon {
  override def getSparkSession(): SparkSession = session
}
