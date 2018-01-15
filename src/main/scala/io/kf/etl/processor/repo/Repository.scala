package io.kf.etl.processor.repo

import java.net.URL

import io.kf.etl.context.Context
import org.apache.hadoop.fs.FileSystem


sealed trait Repository {

}

class HDFSRepository(val url: URL, val fs: FileSystem) extends Repository {

}

class LocalRepository(val url: URL) extends Repository {

}


object Repository {

  class UnrecognizedRepositoryProtocolException(val protocol:String) extends Exception("Unrecognized Repository Protocol: ")

  def apply(url: URL): Repository = {
    url.getProtocol match {
      case "hdfs" => new HDFSRepository(url, Context.hdfs)
      case "file" => new LocalRepository(url)
      case _ => throw new UnrecognizedRepositoryProtocolException(url.getProtocol)
    }
  }
}