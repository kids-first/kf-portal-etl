package io.kf.etl.processors.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext}
import io.kf.etl.common.Constants._

class KfHdfsParquetData(override val sqlContext: SQLContext, override val schema: StructType, val path:String) extends BaseRelation with TableScan{

  override def buildScan(): RDD[Row] = {
    sqlContext.read.parquet(path).rdd

  }
}

object KfHdfsParquetData {
  def apply(sqlContext: SQLContext, schema: StructType, path:String):KfHdfsParquetData = {
    new KfHdfsParquetData(sqlContext, schema, path)
  }

  implicit class KfHdfsParquetDataWrapper(reader: DataFrameReader){
    def kfHdfsParquet(path: String): DataFrame = {
      reader.format(HDFS_DATASOURCE_SHORT_NAME).load(path)
    }
  }
}