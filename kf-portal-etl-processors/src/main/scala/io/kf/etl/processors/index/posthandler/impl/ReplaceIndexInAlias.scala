package io.kf.etl.processors.index.posthandler.impl

import io.kf.etl.processors.index.context.IndexContext
import io.kf.etl.processors.index.posthandler.IndexProcessorPostHandler
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest

import scala.collection.convert.WrapAsScala

class ReplaceIndexInAlias(context: IndexContext, alias: String) extends IndexProcessorPostHandler{

  override def post(): Unit = {

    val remove_request = new IndicesAliasesRequest

      WrapAsScala.asScalaIterator(
        context.appContext.esClient.admin().indices().getAliases((new GetAliasesRequest).aliases(alias)).actionGet().getAliases.iterator()
      ).foreach(tuple => {
        val index_name = tuple.key
        val aliases = WrapAsScala.asScalaBuffer(tuple.value).map(_.alias())

        aliases.foreach(aliasIn => {
          remove_request.addAliasAction(AliasActions.remove().index(index_name).alias(aliasIn))
        })
      })

    val remove_response = context.appContext.esClient.admin().indices().aliases(remove_request).actionGet()

    remove_response.isAcknowledged match {
      case true => {
        val add_req = new IndicesAliasesRequest

        context.appContext.esClient.admin().indices().aliases(add_req.addAliasAction(AliasActions.add().index(s"${alias}_${context.config.releaseTag.releaseTag()}").alias(alias))).actionGet()
      }
      case false => {
        throw new Exception("failed to remove the existing relationship between index and alias")
      }
    }



  }
}
