package io.kf.etl.processor.stage

import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

case class StageJobContext(val sparkSession: SparkSession, val fs:FileSystem, val root_path:String, val config: Option[Config])
