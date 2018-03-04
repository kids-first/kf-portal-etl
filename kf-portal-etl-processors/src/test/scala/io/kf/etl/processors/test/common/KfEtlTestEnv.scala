package io.kf.etl.processors.test.common

import io.kf.test.Doc
import org.apache.spark.sql.SparkSession
import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, PopularProperties}

object KfEtlTestEnv {

  lazy val elasticsearch = createEmbeddedElasticsearch()

  private def createEmbeddedElasticsearch(): EmbeddedElastic = {
    EmbeddedElastic.builder()
      .withElasticVersion("5.6.5")
      .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
      .withSetting(PopularProperties.CLUSTER_NAME, "kf-es-cluster")
      .build()
  }

  lazy val mock_Doc_Entity = createMockDocEntity()

  private def createMockDocEntity(): Doc = {

    Doc(
      createdDatetime = Some("mock-datetime"),
      dataCategory = "dataCategory",
      dataFormat = "dataFormat",
      dataType = "dataType",
      experimentalStrategy = "experimentalStrategy",
      fileName = "fileName",
      fileSize = 100,
      md5Sum = "md5Sum",
      submitterId = "submitterId"
    )

  }

  lazy val spark = createSparkSession()

  private def createSparkSession(): SparkSession = {
    SparkSession.builder().master("local[*]").appName("kf-test").config("es.index.auto.create", "true").config("es.nodes", "localhost").getOrCreate()
  }

}
