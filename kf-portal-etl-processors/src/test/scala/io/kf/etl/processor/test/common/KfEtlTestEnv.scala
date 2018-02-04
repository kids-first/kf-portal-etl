package io.kf.etl.processor.test.common

import com.trueaccord.scalapb.json.JsonFormat
import io.kf.model.Doc
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods
import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, PopularProperties}

import scala.collection.mutable.Map
import scala.io.Source

object KfEtlTestEnv {

  lazy val elasticsearch = createEmbeddedElasticsearch()

  private def createEmbeddedElasticsearch(): EmbeddedElastic = {
    EmbeddedElastic.builder()
      .withElasticVersion("5.0.0")
      .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
      .withSetting(PopularProperties.CLUSTER_NAME, "kf-es-cluster")
      .build()
      .start()
  }

  private lazy val hdfs = createHdfsMap()

  private def createHdfsMap(): Map[String, FileSystem] = {
    Map[String, FileSystem]()
  }

  def getHdfs(url: String = "hdfs://localhost:8020"): FileSystem = {

    hdfs.get(url) match {
      case Some(fs) => fs
      case None => {
        val conf = new Configuration()
        conf.set("fs.defaultFS", url)
        val fs = FileSystem.get(conf)
        hdfs.put(url, fs)
        fs
      }
    }

  }

  lazy val fake_Doc_Entity = createFakeDocEntity()

  private def createFakeDocEntity(): Doc = {

    JsonFormat.fromJson[Doc](
      JsonMethods.parse(
        Source.fromInputStream(
          KfEtlTestEnv.getClass.getResourceAsStream("/mock_doc_entity.json")
        ).mkString
      )
    )
  }

  lazy val spark = createSparkSession()

  private def createSparkSession(): SparkSession = {
    SparkSession.builder().master("local[*]").appName("kf-test").config("es.index.auto.create", "true").getOrCreate()
  }

}
