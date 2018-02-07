package io.kf.etl.common.url

import java.net.{URL, URLConnection, URLStreamHandler}

class Handler extends URLStreamHandler{
  override def openConnection(url: URL): URLConnection = {
    new ClasspathURLConnection(url)
  }
}
