package io.kf.etl.processors.index

import com.typesafe.config.Config
import io.kf.etl.context.DefaultContext
import io.kf.etl.processors.index.mapping.MappingFiles
import org.apache.spark.sql.Dataset
import org.elasticsearch.spark.sql._
import play.api.libs.ws.DefaultBodyWritables.writeableOf_String
import play.api.libs.ws.StandaloneWSClient

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt


object IndexProcessor {
  def apply[T](indexType: String, studyId: String, releaseId: String, dataset: Dataset[T])(implicit wsClient: StandaloneWSClient, config: Config): Unit = {
    val indexName = getIndexName(indexType, studyId, releaseId)
    createMapping(indexName, indexType)
    dataset.toDF().saveToEs(s"$indexName/$indexType", Map("es.mapping.id" -> "kf_id"))
//    dataset.toDF().saveToEs(s"$indexName", Map("es.mapping.id" -> "kf_id"))

  }

  def getIndexName(indexType: String, studyId: String, releaseId: String): String = {
    s"${indexType}_${studyId}_$releaseId".toLowerCase
  }


  private def createMapping(indexName: String, indexType: String)(implicit wsClient: StandaloneWSClient, config: Config): Unit = {
    val content = MappingFiles.getMapping(indexType)

    val elasticSearchUrl = DefaultContext.elasticSearchUrl(config)
    val response = Await.result(wsClient
      .url(s"$elasticSearchUrl/$indexName")
      .withHttpHeaders("Content-Type" -> "application/json")
      .put(content), 30 seconds
    )

    if (response.status != 200) {
      throw new IllegalStateException(s"Impossible to create index :${response} for index:${indexName}")
    }
  }
}
