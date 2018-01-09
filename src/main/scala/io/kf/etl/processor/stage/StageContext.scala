package io.kf.etl.processor.stage

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

case class StageContext(val sparkSession: SparkSession, val config: Option[Config])
