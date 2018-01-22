package io.kf.etl.processor.datasource

import io.kf.etl.datasource.KfDataProviderParametersMissingException
import io.kf.etl.transform.ProtoBuf2StructType
import io.kf.model.Doc
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class KfHdfsDataProvider extends RelationProvider with DataSourceRegister{

  private val must_have_options = Set("kf.etl.hdfs.fs.data.fullpath", "kf.etl.processor.name")

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

    parameters.get("kf.etl.processor.name").collect({
      case "document" => KfHdfsParquetData[Doc](sqlContext, ProtoBuf2StructType.parseDescriptor(Doc.scalaDescriptor), "", classOf[Doc])
      case "index" => KfHdfsParquetData[Doc](sqlContext, ProtoBuf2StructType.parseDescriptor(Doc.scalaDescriptor), "", classOf[Doc])
      case _ => KfHdfsParquetData[Doc](sqlContext, ProtoBuf2StructType.parseDescriptor(Doc.scalaDescriptor), "", classOf[Doc])
    }).get

  }

  override def shortName(): String = "kf-hdfs"


}
