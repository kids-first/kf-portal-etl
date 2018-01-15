package io.kf.etl.processor.stage

import java.net.URL

import com.google.gson.GsonBuilder
import io.kf.etl.processor.repo.Repository
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}


class StageJobTransformer(val context:StageJobContext) {

  def transform(repo: Repository): Dataset[_] = {

    ???
  }

  private def process[T](spark: SparkSession, file: URL, clazz:Class[T]):Dataset[T] ={
    import spark.implicits._

    implicit val encoder = Encoders.bean(clazz)
    spark.readStream.option("multiLine", true).json(s"${file.getProtocol}://${file.getPath}").as[String].mapPartitions(partition => {
      val gson = new GsonBuilder().create()
      partition.map(str => gson.fromJson(str, clazz))
    })
  }

}
