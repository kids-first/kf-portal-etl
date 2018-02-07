package io.kf.etl.datasource

import io.kf.etl.common.Constants.RAW_DATASOURCE_SHORT_NAME
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

class KfRawData(override val sqlContext: SQLContext, override val schema: StructType) extends BaseRelation with TableScan{

  override def buildScan(): RDD[Row] = ???


}

object KfRawData {
  def apply(sc: SQLContext, schema: StructType): KfRawData = {
    new KfRawData(sc, schema)
  }

  implicit class KfRawDataWrapper(reader: DataFrameReader) {
    def kfRaw(path: String):DataFrame = {
      reader.format(RAW_DATASOURCE_SHORT_NAME).load(path)
    }
  }
}
