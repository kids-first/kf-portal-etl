package io.kf.etl.processor.index

import java.net.URL

import com.typesafe.config.Config
import io.kf.etl.conf.ESConfig
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

class IndexJobContext(val sparkSession: SparkSession, val fs:FileSystem, val esConfig: ESConfig, val root_path:URL, val config: Option[Config]) {

}
