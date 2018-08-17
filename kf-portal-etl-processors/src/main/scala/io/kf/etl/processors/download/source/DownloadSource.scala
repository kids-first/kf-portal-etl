package io.kf.etl.processors.download.source

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityEndpointSet
import io.kf.etl.processors.download.context.DownloadContext

class DownloadSource(val context: DownloadContext) {

  def makeEndpointSeq(path: String, ids: Seq[String]): Seq[String] = {
    ids.map(id => s"${path}?study_id=${id}")
  }

  def getEntitySet(input: Option[Array[String]]): EntityEndpointSet = {
    input match {
      case None => {
        val query = "?limit=100"
        EntityEndpointSet(
          participants          = Seq(s"/participants${query}"),
          families              = Seq(s"/families${query}"),
          biospecimens          = Seq(s"/biospecimens${query}"),
          diagnoses             = Seq(s"/diagnoses${query}"),
          familyRelationships   = Seq(s"/family-relationships${query}"),
          genomicFiles          = Seq(s"/genomic-files${query}"),
          investigators         = Seq(s"/investigators${query}"),
          outcomes              = Seq(s"/outcomes${query}"),
          phenotypes            = Seq(s"/phenotypes${query}"),
          sequencingExperiments = Seq(s"/sequencing-experiments${query}"),
          studies               = Seq(s"/studies${query}"),
          studyFiles            = Seq(s"/study-files${query}")
        )
      }
      case Some(ids) => {
        EntityEndpointSet(
          participants          = makeEndpointSeq("/participants",ids),
          families              = makeEndpointSeq("/families",ids),
          biospecimens          = makeEndpointSeq("/biospecimens",ids),
          diagnoses             = makeEndpointSeq("/diagnoses",ids),
          familyRelationships   = makeEndpointSeq("/family-relationships",ids),
          genomicFiles          = makeEndpointSeq("/genomic-files",ids),
          investigators         = makeEndpointSeq("/investigators",ids),
          outcomes              = makeEndpointSeq("/outcomes",ids),
          phenotypes            = makeEndpointSeq("/phenotypes",ids),
          sequencingExperiments = makeEndpointSeq("/sequencing-experiments",ids),
          studies               = makeEndpointSeq("/studies",ids),
          studyFiles            = makeEndpointSeq("/study-files",ids)
        )
      }
    }
  }
}
