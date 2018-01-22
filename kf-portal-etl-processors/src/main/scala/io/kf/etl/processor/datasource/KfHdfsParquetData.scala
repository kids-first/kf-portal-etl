package io.kf.etl.processor.datasource

import io.kf.model.Doc
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row, SQLContext}

class KfHdfsParquetData[T](override val sqlContext: SQLContext, override val schema: StructType, val path:String, val clazz: Class[T]) extends BaseRelation with TableScan{

  override def buildScan(): RDD[Row] = {
    // put the whole instance of T into a Row
    // in order to avoid transform a schema-enabled Row to T instance in the future transformations
    // because Dataset[T] is preferred
    sqlContext.read.parquet(path).as(Encoders.bean(clazz)).rdd.map(Row(_))
  }
}

object KfHdfsParquetData {
  def apply[T](sqlContext: SQLContext, schema: StructType, path:String, clazz: Class[T]):KfHdfsParquetData[T] = {
    new KfHdfsParquetData[T](sqlContext, schema, path, clazz)
  }
}