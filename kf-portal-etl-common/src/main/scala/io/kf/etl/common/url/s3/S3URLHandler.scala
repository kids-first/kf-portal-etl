package io.kf.etl.common.url.s3

import java.io.InputStream
import java.net.{URL, URLConnection, URLStreamHandler}

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

class S3URLHandler(s3:AmazonS3) extends URLStreamHandler{
  override def openConnection(url: URL): URLConnection = {
    new URLConnection(url) {
      override def connect(): Unit = {}

      override def getInputStream: InputStream = {
        val bucket = url.getHost
        val key = url.getPath.charAt(0) match {
          case '/' => url.getPath.substring(1)
          case _ => url.getPath
        }
        s3.getObject(bucket, key).getObjectContent
      }
    }
  }

}
