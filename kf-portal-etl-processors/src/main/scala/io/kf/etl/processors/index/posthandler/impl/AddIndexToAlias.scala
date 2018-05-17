package io.kf.etl.processors.index.posthandler.impl

import io.kf.etl.context.Context
import io.kf.etl.processors.index.context.IndexContext
import io.kf.etl.processors.index.posthandler.IndexProcessorPostHandler
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions

class AddIndexToAlias(context: IndexContext, alias:String) extends IndexProcessorPostHandler{
  override def post(): Unit = {
    val request = new IndicesAliasesRequest
    request.addAliasAction(AliasActions.add().index(s"${alias}_${context.config.releaseTag.releaseTag()}").alias(alias))
    val resp = Context.esClient.admin().indices().aliases(request).actionGet()
    resp.isAcknowledged
  }
}
