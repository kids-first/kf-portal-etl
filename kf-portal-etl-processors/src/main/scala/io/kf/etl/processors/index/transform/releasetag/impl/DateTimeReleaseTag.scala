package io.kf.etl.processors.index.transform.releasetag.impl

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import io.kf.etl.processors.index.transform.releasetag.ReleaseTag

/**
  * This class generate a release tag whose format is java date time string
  * the tag will be appended to index name
  * @param properties Date time string pattern, refer to https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
  */
class DateTimeReleaseTag(val properties: Map[String, String]) extends ReleaseTag {
  lazy override val releaseTag = getTag()

  private def getTag():String = {

    LocalDateTime.now().format(DateTimeFormatter.ofPattern(
      properties.get("pattern") match {
        case Some(p) => p
        case None => "yyyy_MM_dd"
      }
    ))
  }
}
