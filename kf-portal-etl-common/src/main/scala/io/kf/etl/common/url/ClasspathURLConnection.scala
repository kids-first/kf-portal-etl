package io.kf.etl.common.url

import java.io.InputStream
import java.net.{URL, URLConnection}

class ClasspathURLConnection(url: URL) extends URLConnection(url){
  override def connect(): Unit = {
    getClass.getResource(url.getPath).openConnection()
  }

  override def getInputStream: InputStream = {
    getClass.getResourceAsStream(url.getPath)
  }
}
