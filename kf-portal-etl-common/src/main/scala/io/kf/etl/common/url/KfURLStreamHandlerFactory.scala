package io.kf.etl.common.url

import java.net.{URLStreamHandler, URLStreamHandlerFactory}

import com.amazonaws.services.s3.AmazonS3
import io.kf.etl.common.url.classpath.ClasspathURLHandler
import io.kf.etl.common.url.s3.S3URLHandler

class KfURLStreamHandlerFactory(awsS3: AmazonS3) extends URLStreamHandlerFactory {
  override def createURLStreamHandler(protocol: String): URLStreamHandler = {
    val s3 = "s3(.?)".r
    protocol match {
      case "classpath" => new ClasspathURLHandler
      case s3(c) => new S3URLHandler(awsS3)
      case _ => null
    }
  }
}
