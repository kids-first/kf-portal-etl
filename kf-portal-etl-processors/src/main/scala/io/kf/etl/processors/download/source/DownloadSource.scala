package io.kf.etl.processors.download.source

import java.net.URL

import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityEndpointSet
import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.processors.download.dump.DataSourceDump
import io.kf.etl.processors.repo.Repository


class DownloadSource(val context: DownloadContext) {

  def getEntitySet(placeholder:Unit): EntityEndpointSet = {

    EntityEndpointSet(
      participants = "/participants",
      families = "/families",
      biospecimens = "/biospecimens",
      diagnoses = "/diagnoses",
      familyRelationships = "/family-relationships",
      genomicFiles = "/genomic-files",
      investigators = "/investigators",
      outcomes = "/outcomes",
      phenotypes = "/phenotypes",
      sequencingExperiments = "//sequencing-experiments",
      studies = "/studies",
      studyFiles = "/study-files"
    )
  }
}
