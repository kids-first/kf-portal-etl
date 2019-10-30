package io.kf.etl.processors.index

import io.kf.etl.processors.index.mapping.MappingFiles
import org.apache.spark.sql.Dataset
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.spark.sql._
import org.json4s.jackson.JsonMethods._


object IndexProcessor {
  def apply[T](indexType: String, studyId: String, releaseId: String, dataset: Dataset[T])(implicit esClient: TransportClient): Unit = {
    val indexName = getIndexName(indexType, studyId, releaseId)
    createMapping(indexName, indexType)
    dataset.toDF().saveToEs(s"$indexName/$indexType", Map("es.mapping.id" -> "kf_id"))

  }

  def getIndexName(indexType: String, studyId: String, releaseId: String): String = {
    s"${indexType}_${studyId}_$releaseId".toLowerCase
  }


  private def createMapping(indexName:String, indexType:String)(implicit esClient: TransportClient):Unit = {
    val content = MappingFiles.getMapping(indexType)

    val jvalue = parse(content)

    val mappings = compact(render(jvalue \ "mappings" \ indexType))
    val settings = compact(render(jvalue \ "settings"))


    esClient.admin().indices()
      .prepareCreate(indexName)
      .setSettings(settings, XContentType.JSON)
      .addMapping(indexType, mappings, XContentType.JSON)
      .get()

    println(s"Successfully created index $indexName")

  }
}