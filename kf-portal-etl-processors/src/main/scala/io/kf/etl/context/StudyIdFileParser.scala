package io.kf.etl.context

import java.net.URL

import scala.io.Source

object StudyIdFileParser {
  def getStudyIDs(url: URL): Array[String] = {

    Source.fromURL(url).getLines().map(_.trim).toArray

//    url.getProtocol match {
//      case "file" => {
//        Source.fromFile(new File( url.getFile )).getLines().map(_.trim).toArray
//      }
//      case "s3" => {
//        Source.fromURL(url).getLines().map(_.trim).toArray
//      }
//      case _ => {
//        throw new Exception(s"URL protocol '${url.getProtocol}' is not supported at this time")
//      }
//    }
  }
}
