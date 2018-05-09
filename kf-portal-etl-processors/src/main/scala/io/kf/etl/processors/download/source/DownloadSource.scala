package io.kf.etl.processors.download.source

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityEndpointSet
import io.kf.etl.processors.download.context.DownloadContext

class DownloadSource(val context: DownloadContext) {

  def getEntitySet(input:Map[String,String]): EntityEndpointSet = {


    val query =
      input.map(tuple => {
        s"${tuple._1}=${tuple._2}"
      }).mkString("?", "&", "") match {
        case "?" => ""
        case ret:String => ret
      }

    EntityEndpointSet(
      participants = s"/participants${query}",
      families = s"/families${query}",
      biospecimens = s"/biospecimens${query}",
      diagnoses = s"/diagnoses${query}",
      familyRelationships = s"/family-relationships${query}",
      genomicFiles = s"/genomic-files${query}",
      investigators = s"/investigators${query}",
      outcomes = s"/outcomes${query}",
      phenotypes = s"/phenotypes${query}",
      sequencingExperiments = s"/sequencing-experiments${query}",
      studies = s"/studies${query}",
      studyFiles = s"/study-files${query}"
    )
  }
}
