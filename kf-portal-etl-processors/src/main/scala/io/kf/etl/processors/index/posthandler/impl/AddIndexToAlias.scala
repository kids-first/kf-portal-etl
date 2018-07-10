package io.kf.etl.processors.index.posthandler.impl

import io.kf.etl.processors.index.context.IndexContext
import io.kf.etl.processors.index.posthandler.{IndexProcessorPostHandler, unused}
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions

@unused
class AddIndexToAlias(context: IndexContext, alias:String) extends IndexProcessorPostHandler{
  override def post(): Unit = {
    val request = new IndicesAliasesRequest
    request.addAliasAction(AliasActions.add().index(s"${alias}_${context.config.releaseTag.releaseTag()}").alias(alias))
    val resp = context.appContext.esClient.admin().indices().aliases(request).actionGet()
    resp.isAcknowledged
  }
}
