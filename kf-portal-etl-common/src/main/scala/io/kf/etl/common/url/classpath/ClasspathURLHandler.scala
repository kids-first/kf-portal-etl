package io.kf.etl.common.url.classpath

import java.net.{URL, URLConnection, URLStreamHandler}

class ClasspathURLHandler extends URLStreamHandler{
  override def openConnection(url: URL): URLConnection = {
    new ClasspathURLConnection(url)
  }
}
