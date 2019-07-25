package io.kf.etl.processors.download.source

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityEndpointSet
import io.kf.etl.processors.download.context.DownloadContext

class DownloadSource(val context: DownloadContext) {

  def makeEndpointSeq(studyId: String, path: String): String = s"$path?study_id=$studyId"

  def getEntitySet(studyId: String): EntityEndpointSet = {
    EntityEndpointSet(
      participants = makeEndpointSeq(studyId, "/participants"),
      families = makeEndpointSeq(studyId, "/families"),
      biospecimens = makeEndpointSeq(studyId, "/biospecimens"),
      biospecimenGenomicFiles = makeEndpointSeq(studyId, "/biospecimen-genomic-files"),
      biospecimenDiagnoses = makeEndpointSeq(studyId, "/biospecimen-diagnoses"),
      diagnoses = makeEndpointSeq(studyId, "/diagnoses"),
      familyRelationships = makeEndpointSeq(studyId, "/family-relationships"),
      genomicFiles = makeEndpointSeq(studyId, "/genomic-files"),
      investigators = makeEndpointSeq(studyId, "/investigators"),
      outcomes = makeEndpointSeq(studyId, "/outcomes"),
      phenotypes = makeEndpointSeq(studyId, "/phenotypes"),
      sequencingExperiments = makeEndpointSeq(studyId, "/sequencing-experiments"),
      sequencingExperimentGenomicFiles = makeEndpointSeq(studyId, "/sequencing-experiment-genomic-files"),
      studies = makeEndpointSeq(studyId, "/studies"),
      studyFiles = makeEndpointSeq(studyId, "/study-files")
    )
  }
}
