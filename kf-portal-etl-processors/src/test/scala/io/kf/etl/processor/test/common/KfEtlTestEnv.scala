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
      .withElasticVersion("5.6.5")
      .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
      .withSetting(PopularProperties.CLUSTER_NAME, "kf-es-cluster")
      .build()
//      .start()
  }

  lazy val mock_Doc_Entity = createMockDocEntity()

  private def createMockDocEntity(): Doc = {

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
    SparkSession.builder().master("local[*]").appName("kf-test").config("es.index.auto.create", "true").config("es.nodes", "localhost").getOrCreate()
  }

}
