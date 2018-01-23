package io.kf.etl.common.datasource

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import io.kf.etl.common.Constants._
import io.kf.etl.common.transform.ProtoBuf2StructType
import io.kf.model.Doc

class KfRawDataProvider extends RelationProvider with DataSourceRegister {

  private val must_have_options = Set("")

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    val missingKeys =
      must_have_options.flatMap(key => {
        if(parameters.get(key).isDefined)
          Set("")
        else
          Set(key)
      })
    if(missingKeys.isEmpty) {
      throw KfDataProviderParametersMissingException(missingKeys)
    }

    KfRawData(
      sqlContext,
      ProtoBuf2StructType.parseDescriptor(Doc.scalaDescriptor)
    )
  }

  override def shortName(): String = RAW_DATASOURCE_SHORT_NAME
}
