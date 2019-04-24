package io.kf.etl.processors.download.sink

import java.net.URL

import io.kf.etl.common.Constants.{HPO_GRAPH_PATH, HPO_TERMS, MONDO_TERMS, NCIT_TERMS}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.{DataServiceEntityNames, EntityDataSet}
import io.kf.etl.processors.common.ops.URLPathOps
import io.kf.etl.processors.download.context.DownloadContext

class DownloadSink(val context: DownloadContext) {

  def sink(data: EntityDataSet): EntityDataSet = {

    URLPathOps.removePathIfExists(new URL(context.getJobDataPath()), context.appContext)

    data.participants.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Participant}")
    data.families.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Family}")
    data.biospecimens.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Biospecimen}")
    data.diagnoses.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Diagnosis}")
    data.familyRelationships.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Family_Relationship}")
    data.genomicFiles.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Genomic_File}")
    data.sequencingExperiments.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Sequencing_Experiment}")
    data.studies.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Study}")
    data.studyFiles.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Study_File}")
    data.investigators.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Investigator}")
    data.outcomes.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Outcome}")
    data.phenotypes.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Phenotype}")
    data.ontologyData.hpoTerms.write.parquet(s"${context.getJobDataPath()}/${HPO_TERMS}")
    data.ontologyData.mondoTerms.write.parquet(s"${context.getJobDataPath()}/${MONDO_TERMS}")
    data.ontologyData.ncitTerms.write.parquet(s"${context.getJobDataPath()}/${NCIT_TERMS}")

    data
  }
}
