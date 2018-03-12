package io.kf.etl.processors.index.transform.releasetag.impl

import io.kf.etl.processors.index.transform.releasetag.ReleaseTag

class AnyStringSuffixReleaseTag(val properties: Map[String, String]) extends ReleaseTag {

  lazy override val releaseTag = getTag()

  private def getTag():String = {
    properties.get("pattern") match {
      case Some(p) => p
      case None => "test"
    }
  }
}
