package io.kf.etl.processor.stage

import java.net.URL

import com.google.gson.GsonBuilder
import io.kf.etl.processor.Repository
import io.kf.model.{Edge, Node, Project}
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}


class StageTransformer(val context:StageContext) {

  def transform(repo: Repository): Dataset[Project] = {
    repo.getPrograms().map(program => {
      repo.getProjectsByProgram(program._2).map(project => {
        val files = repo.getFilesByProject(project._2).toMap

        val sar = files.get("submitted_aligned_read").get
        val rg = files.get("read_group").get
        val sar_rg = files.get("sar_rg").get

        val spark = context.sparkSession
        import spark.implicits._

        val sarDS =
          spark.readStream.textFile(s"${sar.getProtocol}://${sar.getPath}").map(line => {
            val fields = line.split('\t')
            Node(fields(0), fields(1))
          })

        val rgDS =
          spark.readStream.textFile(s"${rg.getProtocol}://${rg.getPath}").map(line => {
            val fields = line.split('\t')
            Node(fields(0), fields(1))
          })

        val sar_rgDS =
          spark.readStream.textFile(s"${sar_rg.getProtocol}://${sar_rg.getPath}").map(line => {
            val fields = line.split('\t')
            Edge(fields(0), fields(1))
          })

        val rets =
          sarDS.withColumnRenamed("id", "left").withColumnRenamed("content", "left_content").join(
            sar_rgDS,
            Seq("left"),
            "left_outer"
          ).join(
            rgDS.withColumnRenamed("id", "right").withColumnRenamed("content", "right_content"),
            Seq("right"),
            "left_outer"
          ).groupByKey(_.getAs[String]("right"))

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
