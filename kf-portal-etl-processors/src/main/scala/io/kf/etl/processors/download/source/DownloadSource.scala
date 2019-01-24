package io.kf.etl.processors.download.source

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityEndpointSet
import io.kf.etl.processors.download.context.DownloadContext

class DownloadSource(val context: DownloadContext) {

  def makeEndpointSeq(studyIds: Option[Array[String]], path: String): Seq[String] = {
    studyIds match {
      case None       => Seq(s"${path}?limit=100")
      case Some(ids)  => ids.map(id => s"${path}?study_id=${id}")
    }
  }

  def getEntitySet(studyIds: Option[Array[String]]): EntityEndpointSet = {
    EntityEndpointSet(
      participants                      = makeEndpointSeq(studyIds, "/participants"),
      families                          = makeEndpointSeq(studyIds, "/families"),
      biospecimens                      = makeEndpointSeq(studyIds, "/biospecimens"),
      biospecimenGenomicFiles           = makeEndpointSeq(studyIds, "/biospecimen-genomic-files"),
      diagnoses                         = makeEndpointSeq(studyIds, "/diagnoses"),
      familyRelationships               = makeEndpointSeq(studyIds, "/family-relationships"),
      genomicFiles                      = makeEndpointSeq(studyIds, "/genomic-files"),
      investigators                     = makeEndpointSeq(studyIds, "/investigators"),
      outcomes                          = makeEndpointSeq(studyIds, "/outcomes"),
      phenotypes                        = makeEndpointSeq(studyIds, "/phenotypes"),
      sequencingExperiments             = makeEndpointSeq(studyIds, "/sequencing-experiments"),
      sequencingExperimentGenomicFiles  = makeEndpointSeq(studyIds, "/sequencing-experiment-genomic-files"),
      studies                           = makeEndpointSeq(studyIds, "/studies"),
      studyFiles                        = makeEndpointSeq(studyIds, "/study-files")
    )
  }
}
