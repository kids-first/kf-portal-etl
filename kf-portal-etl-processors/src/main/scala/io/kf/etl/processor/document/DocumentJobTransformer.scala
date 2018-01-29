package io.kf.etl.processor.document

import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.{Dataset, SparkSession}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

class DocumentJobTransformer(val spark:SparkSession) {

//  def transform(data:Dataset[Doc]):Dataset[String] = {
//    import spark.implicits._
//    data.map(doc => {
//      implicit val formats = Serialization.formats(NoTypeHints)
//      Serialization.writePretty(doc)
//    })
//  }

  def transform(repo: Repository):Dataset[String] = {
    import spark.implicits._
    val one_path:String = ???
    spark.read.parquet("").as[Doc].map(doc => {
      implicit val formats = Serialization.formats(NoTypeHints)
      Serialization.writePretty(doc)
    })
  }

  def transform(input: Dataset[Doc]): Dataset[Doc] = {
    input
  }

}
