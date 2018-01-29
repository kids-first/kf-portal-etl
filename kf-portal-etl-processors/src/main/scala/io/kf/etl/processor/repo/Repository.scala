package io.kf.etl.processor.repo

import java.net.{URI, URL}

import io.kf.etl.common.context.Context
import io.kf.model.Doc
import org.apache.hadoop.fs.{FileSystem => HDFS}
import org.apache.spark.sql.Dataset

import scala.util.{Failure, Success, Try}


sealed trait Repository {
  def url: URL
  def load[T](): Dataset[T]
}

class HDFSRepository(override val url: URL) extends Repository{
  override def load[T](): Dataset[T] = ???
}

class LocalRepository(override val url: URL) extends Repository {
  override def load[T](): Dataset[T] = ???
}

class PostgresQLRepository(override val url:URL) extends Repository {
  override def load[T](): Dataset[T] = ???
}


object Repository {

  class UnrecognizedRepositoryProtocolException(val protocol:String) extends Exception("Unrecognized Repository Protocol: ")

  def apply(url: URL): Repository = {
    url.toURI.getScheme match {
      case "hdfs" => new HDFSRepository(url)
      case "file" => new LocalRepository(url)
      case "jdbc" => {
        new URI(url.toString.substring(5)).getScheme match {
          case "postgresql" => new PostgresQLRepository(url)
          case _ => throw new UnrecognizedRepositoryProtocolException(url.toString)
        }
      }
      case _ => throw new UnrecognizedRepositoryProtocolException(url.toString)
    }
  }
}