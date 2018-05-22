package io.kf.etl.common.url

import java.net.URL

trait KfURLEnabler {
  KfURLEnabler.getClass.synchronized{
    if(!KfURLEnabler.enabled) {
      URL.setURLStreamHandlerFactory(new KfURLStreamHandlerFactory)
      KfURLEnabler.enabled = true
    }
  }
}

object KfURLEnabler{
  private var enabled = false
}