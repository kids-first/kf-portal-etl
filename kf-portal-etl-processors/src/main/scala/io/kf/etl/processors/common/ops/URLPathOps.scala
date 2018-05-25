package io.kf.etl.processors.common.ops

import java.io.File
import java.net.URL

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.convert.WrapAsScala

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
      val bucket = path.getHost
      val common_path = path.getPath.split('/').filter(!_.trim.isEmpty).mkString("", "/", "/")

      val deleteObjsReq = new DeleteObjectsRequest(bucket).withKeys(
        WrapAsScala.asScalaBuffer( s3.listObjects(bucket).getObjectSummaries ).filter(summary => {
          summary.getKey.startsWith(common_path)
        }).map(_.getKey) : _*
      )

      s3.deleteObjects(deleteObjsReq)
    }
  }
}
