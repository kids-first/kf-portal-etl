package io.kf.etl.common.url

import java.net.{URLStreamHandler, URLStreamHandlerFactory}

/**
  * url format is "classpath:///package1/package2/...
  */

class ClasspathURLStreamHandlerFactory extends URLStreamHandlerFactory{

  private lazy val handler = createHandler()

  private def createHandler(): URLStreamHandler = {
    new Handler
  }
  override def createURLStreamHandler(protocol: String): URLStreamHandler = {
    protocol match {
      case "classpath" => handler
      case _ => null
    }
  }
}
