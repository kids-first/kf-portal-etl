package io.kf.etl.processor.repo

import java.net.{URI, URL}

import io.kf.etl.context.Context
import io.kf.model.Doc
import org.apache.hadoop.fs.{FileSystem => HDFS}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success, Try}


case class Repository(url: URL)

//sealed trait Repository[T] {
//  def url: URL
//  def load(): Dataset[T]
//}
//
//class DefaultHDFSRepository(override val url: URL, val spark: SparkSession) extends Repository[Doc]{
//  override def load(): Dataset[Doc] = {
//    import io.kf.etl.processor.datasource.KfHdfsParquetData._
//    import spark.implicits._
//    spark.read.kfHdfs(url.toString).as[Doc]
//  }
//}
//
//class DefaultLocalRepository(override val url: URL, val spark: SparkSession) extends Repository[Doc] {
//  override def load(): Dataset[Doc] = {
//    import io.kf.etl.processor.datasource.KfHdfsParquetData._
//    import spark.implicits._
//    spark.read.kfHdfs(url.toString).as[Doc]
//  }
//}
//
//class PostgresqlRepository(override val url:URL, val spark: SparkSession) extends Repository[Doc] {
//  override def load(): Dataset[Doc] = ???
//}
//
//
//object Repository {
//
//  class UnrecognizedRepositoryProtocolException(val protocol:String) extends Exception("Unrecognized Repository Protocol: ")
//
//  def apply(url: URL): Repository[Doc] = {
//    url.toURI.getScheme match {
//      case "hdfs" => new DefaultHDFSRepository(url, Context.sparkSession)
//      case "file" => new DefaultLocalRepository(url, Context.sparkSession)
//      case "jdbc" => {
//        new URI(url.toString.substring(5)).getScheme match {
//          case "postgresql" => new PostgresqlRepository(url, Context.sparkSession)
//          case _ => throw new UnrecognizedRepositoryProtocolException(url.toString)
//        }
//      }
//      case _ => throw new UnrecognizedRepositoryProtocolException(url.toString)
//    }
//  }
//}