package io.kf.etl.processors.common.ops

import java.io.File
import java.net.URL

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.{Failure, Success, Try}

object URLPathOps {
  def removePathIfExists(path: URL)(implicit hdfs: FileSystem):Boolean = {
    path.getProtocol match {
      case "file" => {
        new FileURLOps().removePathIfExists(path)
      }
      case "hdfs" => {
        new HDFSURLOps(hdfs).removePathIfExists(path)
      }
      case "s3a" => {
        new S3URLOps().removePathIfExists(path)
      }
    }
  }

  private sealed trait URLOps{
    def removePathIfExists(path:URL):Boolean
  }

  private class FileURLOps extends URLOps {
    override def removePathIfExists(path: URL): Boolean = {
      val dir = new File(path.getFile)
      if(dir.exists())
        FileUtils.deleteDirectory(dir)
      dir.mkdir()
    }
  }

  private class HDFSURLOps(hdfs: FileSystem) extends URLOps {
    override def removePathIfExists(path: URL): Boolean = {
      val dir = new Path(path.toString)
      hdfs.delete(dir, true)
      hdfs.mkdirs(dir)
    }
  }

  private class S3URLOps extends URLOps {
    override def removePathIfExists(path: URL): Boolean = {
      val s3 = AmazonS3ClientBuilder.defaultClient()
      Try(s3.deleteObject(path.getHost, path.getPath)) match {
        case Success(_) => true
        case Failure(_) => false
      }
    }
  }
}
