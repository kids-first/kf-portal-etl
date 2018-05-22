package io.kf.etl.common.url.s3

import java.io.InputStream
import java.net.{URL, URLConnection, URLStreamHandler}

import com.amazonaws.services.s3.AmazonS3ClientBuilder

class S3URLHandler extends URLStreamHandler{
  override def openConnection(url: URL): URLConnection = {
    new URLConnection(url) {
      override def connect(): Unit = {}

      override def getInputStream: InputStream = {
        val s3 = AmazonS3ClientBuilder.defaultClient()

        val bucket = url.getHost
        val path = url.getPath
        s3.getObject(bucket, path).getObjectContent
      }
    }
  }
}
