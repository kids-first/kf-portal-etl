package io.kf.etl.processors.common.ops

import java.io.File
import java.net.URL

import com.amazonaws.services.s3.AmazonS3
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}

object URLPathOps {
  def removePathIfExists(path: URL)(implicit hdfs: FileSystem, s3Client: AmazonS3):Unit = {
    val s3 = "s3(.*)".r
    path.getProtocol match {
      case "file" => {
        new FileURLOps().removePathIfExists(path)
      }
      case "hdfs" => {
        new HDFSURLOps(hdfs).removePathIfExists(path)
      }
      case s3(c) => {
        new S3URLOps(s3Client).removePathIfExists(path)
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

  private class S3URLOps(s3:AmazonS3) extends URLOps {
    override def removePathIfExists(path: URL): Unit = {
      s3.deleteObject(path.getHost, path.getPath)
    }
  }
}
