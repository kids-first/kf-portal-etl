package io.kf.etl.processors.index.transform.releasetag.impl

import io.kf.etl.processors.index.transform.releasetag.ReleaseTag

class EmptyReleaseTag(val properties: Map[String, String]) extends ReleaseTag{
  override def releaseTag(): Option[String] = None
}
