package io.kf.etl.common.url

import java.net.URL

trait ClasspathURLEnabler {
  ClasspathURLEnabler.getClass.synchronized {
    if(!ClasspathURLEnabler.enabled){
      URL.setURLStreamHandlerFactory(new ClasspathURLStreamHandlerFactory)
      ClasspathURLEnabler.enabled = true
    }

  }
}

object ClasspathURLEnabler{
  private var enabled = false
}
