package io.kf.etl.processors.index

import com.typesafe.config.Config
import io.kf.etl.common.Constants.{CONFIG_NAME_ES_PASS, CONFIG_NAME_ES_USER}
import io.kf.etl.common.Utils.getOptionalConfig
import io.kf.etl.context.DefaultContext
import io.kf.etl.processors.index.mapping.MappingFiles
import org.apache.spark.sql.Dataset
import org.elasticsearch.spark.sql._
import play.api.libs.ws.DefaultBodyWritables.writeableOf_String
import play.api.libs.ws.{StandaloneWSClient, WSAuthScheme}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt


object IndexProcessor {
  def apply[T](indexType: String, studyId: String, releaseId: String, dataset: Dataset[T])(implicit wsClient: StandaloneWSClient, config: Config): Unit = {
    val indexName = getIndexName(indexType, studyId, releaseId)
    createMapping(indexName, indexType)
    dataset.toDF().saveToEs(s"$indexName", Map("es.mapping.id" -> "kf_id"))

  }

  def getIndexName(indexType: String, studyId: String, releaseId: String): String = {
    s"${indexType}_${studyId}_$releaseId".toLowerCase
  }


  private def createMapping(indexName: String, indexType: String)(implicit wsClient: StandaloneWSClient, config: Config): Unit = {
    val content = MappingFiles.getMapping(indexType)

    val elasticSearchUrl = DefaultContext.elasticSearchUrl(config)

    val user = getOptionalConfig(CONFIG_NAME_ES_USER, config)
    val pwd = getOptionalConfig(CONFIG_NAME_ES_PASS, config)

    val response = if(user.isDefined && pwd.isDefined){
      Await.result(wsClient
        .url(s"$elasticSearchUrl/$indexName")
        .withHttpHeaders("Content-Type" -> "application/json")
        .withAuth(user.get, pwd.get, WSAuthScheme.BASIC)
        .put(content), 30 seconds)
    } else {
      Await.result(wsClient
        .url(s"$elasticSearchUrl/$indexName")
        .withHttpHeaders("Content-Type" -> "application/json")
        .put(content), 30 seconds)
    }

    if (response.status != 200) {
      throw new IllegalStateException(s"Impossible to create index :${response} for index:${indexName}")
    }
  }
}
