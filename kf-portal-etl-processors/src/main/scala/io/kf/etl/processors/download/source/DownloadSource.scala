package io.kf.etl.processors.download.source

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityEndpointSet
import io.kf.etl.processors.download.context.DownloadContext

class DownloadSource(val context: DownloadContext) {

  def getEntitySet(input: Option[Array[String]]): Seq[EntityEndpointSet] = {
    input match {
      case None => {
        val query = "?limit=100"
        Seq(
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
        )
      }
      case Some(ids) => {
        ids.map(id => {
          val query = s"?study_id=${id}"
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
        })
      }
    }
  }
}
