package io.kf.etl.processors.index.sink

import io.kf.etl.common.conf.ESConfig
import io.kf.etl.processors.index.mapping.MappingFiles
import io.kf.etl.processors.index.transform.releasetag.ReleaseTag
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentType}
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.jackson.JsonMethods._

class IndexSink(val spark:SparkSession, val esConfig: ESConfig, val releaseTagInstance:ReleaseTag, val client: TransportClient) {
  def sink(data:(String, Dataset[String])):Unit = {

    val release_tag = releaseTagInstance.releaseTag
    val index_name = s"${data._1}_${release_tag}"
    val type_name = data._1

    createMapping(data._1, release_tag)

    EsSpark.saveJsonToEs(data._2.rdd, s"${index_name}/${type_name}", Map("es.mapping.id" -> "kf_id"))
  }

  private def createMapping(index_name_prefix:String, release_tag: String):Unit = {
    val content = MappingFiles.getMapping(index_name_prefix)

    val jvalue = parse(content)

    val mappings = compact(render(jvalue \ "mappings" \ index_name_prefix))
    val settings = compact(render(jvalue \ "settings"))

    client.admin().indices()
      .prepareCreate(s"${index_name_prefix}_${release_tag}")
      .setSettings(settings, XContentType.JSON)
      .addMapping(index_name_prefix, mappings, XContentType.JSON)
      .get()

    println(s"Successfully created index ${index_name_prefix}_${release_tag}")

  }
}
