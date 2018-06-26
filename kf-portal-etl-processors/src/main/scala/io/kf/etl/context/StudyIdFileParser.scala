package io.kf.etl.context

import java.net.URL

import scala.io.Source

object StudyIdFileParser {
  def getStudyIDs(url: URL): Array[String] = {

    Source.fromURL(url).getLines().map(_.trim).toArray

  }
}
