package io.kf.etl.common.url

import java.net.URLStreamHandler

import com.amazonaws.services.s3.AmazonS3
import io.kf.etl.common.url.classpath.ClasspathURLHandler
import io.kf.etl.common.url.s3.S3URLHandler
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory

class KfURLStreamHandlerFactory(awsS3:AmazonS3) extends FsUrlStreamHandlerFactory{
  override def createURLStreamHandler(protocol: String): URLStreamHandler = {
    val s3 = "s3(.?)".r
    protocol match {
      case "classpath" => new ClasspathURLHandler
      case s3(c) => new S3URLHandler(awsS3)
      case "hdfs" => super.createURLStreamHandler(protocol)
      case _ => null
    }
  }
}
