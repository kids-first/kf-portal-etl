package io.kf.etl.processors.common.ops

import java.io.File
import java.net.URL

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.{Failure, Success, Try}

object URLPathOps {
  def removePathIfExists(path: URL)(implicit hdfs: FileSystem):Unit = {
    val s3 = "s3(.*)".r
    path.getProtocol match {
      case "file" => {
        new FileURLOps().removePathIfExists(path)
      }
      case "hdfs" => {
        new HDFSURLOps(hdfs).removePathIfExists(path)
      }
      case s3(c) => {
        new S3URLOps().removePathIfExists(path)
      }
      case  _ => throw new Exception(s"${path.getProtocol} is not supported!")
    }
  }

  private sealed trait URLOps{
    def removePathIfExists(path:URL):Unit
  }

  private class FileURLOps extends URLOps {
    override def removePathIfExists(path: URL): Unit = {
      val dir = new File(path.getFile)
      if(dir.exists())
        FileUtils.deleteDirectory(dir)
    }
  }

  private class HDFSURLOps(hdfs: FileSystem) extends URLOps {
    override def removePathIfExists(path: URL): Unit = {
      val dir = new Path(path.toString)
      hdfs.delete(dir, true)
    }
  }

  private class S3URLOps extends URLOps {
    override def removePathIfExists(path: URL): Unit = {
      val s3 = AmazonS3ClientBuilder.defaultClient()
      s3.deleteObject(path.getHost, path.getPath)
    }
  }
}
