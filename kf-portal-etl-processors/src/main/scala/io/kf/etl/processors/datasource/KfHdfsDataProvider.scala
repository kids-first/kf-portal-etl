package io.kf.etl.processors.datasource

import io.kf.etl.datasource.KfDataProviderParametersMissingException
import io.kf.etl.transform.ScalaPB2SparkStructType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import io.kf.etl.common.Constants._
import io.kf.etl.model.FileCentric

class KfHdfsDataProvider extends RelationProvider with DataSourceRegister{

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


    parameters
      .get(DATASOURCE_OPTION_PROCESSOR_NAME)
      .map(_ match {
        case PROCESSOR_DOCUMENT => KfHdfsParquetData(sqlContext, ScalaPB2SparkStructType.parseDescriptor(FileCentric.scalaDescriptor), parameters.get(SPARK_DATASOURCE_OPTION_PATH).get)
        case PROCESSOR_INDEX => KfHdfsParquetData(sqlContext, ScalaPB2SparkStructType.parseDescriptor(FileCentric.scalaDescriptor), parameters.get(SPARK_DATASOURCE_OPTION_PATH).get)
      })
      .getOrElse(KfHdfsParquetData(sqlContext, ScalaPB2SparkStructType.parseDescriptor(FileCentric.scalaDescriptor), parameters.get(SPARK_DATASOURCE_OPTION_PATH).get))

  }

  override def shortName(): String = HDFS_DATASOURCE_SHORT_NAME


}
