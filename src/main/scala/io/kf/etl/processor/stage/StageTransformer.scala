package io.kf.etl.processor.stage

import java.net.URL

import com.google.gson.GsonBuilder
import io.kf.etl.processor.Repository
import io.kf.model.Project
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}


class StageTransformer(val context:StageContext) {

  def transform(repo: Repository): Dataset[Project] = {
    repo.getPrograms().map(program => {
      repo.getProjectsByProgram(program._2).map(project => {
        val files = repo.getFilesByProject(project._2).toMap
      })
    })
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
