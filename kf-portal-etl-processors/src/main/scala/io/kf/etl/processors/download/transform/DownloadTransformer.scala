package io.kf.etl.processors.download.transform

import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, EntityEndpointSet}
import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.external.dataservice.entity._

class DownloadTransformer(val context:DownloadContext) {

  def transform(endpoints: EntityEndpointSet): EntityDataSet = {

    import context.sparkSession.implicits._
    import EntityParentIDExtractor._
    val retrieval = EntityDataRetrieval(context.config.dataService.url)

    val dataset =
      EntityDataSet(
        participants = context.sparkSession.createDataset(retrieval.retrieve[EParticipant](Some(endpoints.participants))).cache(),
        families = context.sparkSession.createDataset(retrieval.retrieve[EFamily](Some(endpoints.families))).cache(),
        biospecimens = context.sparkSession.createDataset(retrieval.retrieve[EBiospecimen](Some(endpoints.biospecimens))).cache(),
        diagnoses = context.sparkSession.createDataset(retrieval.retrieve[EDiagnosis](Some(endpoints.diagnoses))).cache(),
        familyRelationships = context.sparkSession.createDataset(retrieval.retrieve[EFamilyRelationship](Some(endpoints.familyRelationships))).cache(),
        genomicFiles = context.sparkSession.createDataset(retrieval.retrieve[EGenomicFile](Some(endpoints.genomicFiles))).cache(),
        investigators = context.sparkSession.createDataset(retrieval.retrieve[EInvestigator](Some(endpoints.investigators))).cache(),
        outcomes = context.sparkSession.createDataset(retrieval.retrieve[EOutcome](Some(endpoints.outcomes))).cache(),
        phenotypes = context.sparkSession.createDataset(retrieval.retrieve[EPhenotype](Some(endpoints.phenotypes))).cache(),
        sequencingExperiments = context.sparkSession.createDataset(retrieval.retrieve[ESequencingExperiment](Some(endpoints.sequencingExperiments))).cache(),
        studies = context.sparkSession.createDataset(retrieval.retrieve[EStudy](Some(endpoints.studies))).cache(),
        studyFiles = context.sparkSession.emptyDataset[EStudyFile],//context.sparkSession.createDataset(retrieval.retrieve[EStudyFile](Some(endpoints.studyFiles))).cache(),
        graphPath = HPOGraphPath.get(context).cache()
      )
    retrieval.stop()
    dataset
  }
}
