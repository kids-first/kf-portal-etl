package io.kf.etl.processor.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

class KfHdfsParquetData(override val sqlContext: SQLContext, override val schema: StructType, val path:String) extends BaseRelation with TableScan{

  override def buildScan(): RDD[Row] = {
    sqlContext.read.parquet(path).rdd

  }
}

object KfHdfsParquetData {
  def apply[T](sqlContext: SQLContext, schema: StructType, path:String):KfHdfsParquetData = {
    new KfHdfsParquetData(sqlContext, schema, path)
  }
}