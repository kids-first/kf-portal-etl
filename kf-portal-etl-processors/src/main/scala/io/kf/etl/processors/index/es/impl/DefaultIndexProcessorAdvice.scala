package io.kf.etl.processors.index.es.impl

import io.kf.etl.context.Context
import io.kf.etl.processors.index.context.IndexContext
import io.kf.etl.processors.index.es.IndexProcessorAdvice
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest

class DefaultIndexProcessorAdvice extends IndexProcessorAdvice[String, String]{
  override def preProcess(context: IndexContext, data: String): Unit = {
    val request = new DeleteIndexRequest(s"${data}_${context.config.releaseTag.releaseTag()}")
    val resp = Context.esClient.admin().indices().delete(request).actionGet()

    resp.isAcknowledged
  }

  override def postProcess(context: IndexContext, data: String): Unit = {
    val request = new IndicesAliasesRequest
    request.addAliasAction(AliasActions.add().index(s"${data}_${context.config.releaseTag.releaseTag()}").alias(data))
    val resp = Context.esClient.admin().indices().aliases(request).actionGet()
    resp.isAcknowledged
  }
}
