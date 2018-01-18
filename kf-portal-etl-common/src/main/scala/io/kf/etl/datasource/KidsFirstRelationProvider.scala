package io.kf.etl.datasource

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class KidsFirstRelationProvider extends RelationProvider with DataSourceRegister with KidsFirstRelationProviderOptions {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    val missingKeys =
      must_have_options.keySet.flatMap(key => {
        if(parameters.get(key).isDefined)
          Set("")
        else
          Set(key)
      })
    if(missingKeys.isEmpty) {
      throw KidsFirstRelationProviderParametersMissingException(missingKeys)
    }

    KidsFirstRelation(sqlContext)
  }

  override def shortName(): String = "kids-first"
}
