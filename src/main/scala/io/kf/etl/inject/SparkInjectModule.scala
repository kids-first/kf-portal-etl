package io.kf.etl.inject

import com.google.inject.{AbstractModule, Provides}
import io.kf.etl.conf.SparkConfig
import org.apache.spark.sql.SparkSession

class SparkInjectModule(private val spark: SparkConfig) extends AbstractModule{
  override def configure(): Unit = ???


  @Provides
  def createSparkSession():SparkSession = {
    SparkSession.builder().master(spark.master).appName(spark.appName).getOrCreate()
  }
}
