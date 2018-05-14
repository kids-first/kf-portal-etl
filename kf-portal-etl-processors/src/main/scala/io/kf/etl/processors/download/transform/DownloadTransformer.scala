package io.kf.etl.processors.download.transform

import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, EntityEndpointSet}
import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.external.dataservice.entity._

class DownloadTransformer(val context:DownloadContext) {

  def transform(endpoints: Seq[EntityEndpointSet]): EntityDataSet = {

    import context.sparkSession.implicits._
    import EntityParentIDExtractor._
    val retrieval = EntityDataRetrieval(context.config.dataService.url)

    val dataset =
      EntityDataSet(
        participants = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[EParticipant](Some(ees.participants)))).foldLeft(context.sparkSession.emptyDataset[EParticipant]){
            (left, right) => left.union(right)
          }.cache()
        },
        families = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[EFamily](Some(ees.families)))).foldLeft(context.sparkSession.emptyDataset[EFamily]){
            (left, right) => left.union(right)
          }.cache()
        },
        biospecimens = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[EBiospecimen](Some(ees.biospecimens)))).foldLeft(context.sparkSession.emptyDataset[EBiospecimen]){
            (left, right) => left.union(right)
          }.cache()
        },
        diagnoses = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[EDiagnosis](Some(ees.diagnoses)))).foldLeft(context.sparkSession.emptyDataset[EDiagnosis]){
            (left, right) => left.union(right)
          }.cache()
        },
        familyRelationships = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[EFamilyRelationship](Some(ees.familyRelationships)))).foldLeft(context.sparkSession.emptyDataset[EFamilyRelationship]){
            (left, right) => left.union(right)
          }.cache()
        },
        genomicFiles = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[EGenomicFile](Some(ees.genomicFiles)))).foldLeft(context.sparkSession.emptyDataset[EGenomicFile]){
            (left, right) => left.union(right)
          }.filter(f => {
            f.dataType match {
              case Some(data_type) => {
                !data_type.toLowerCase.split(' ').takeRight(1)(0).equals("index")
              }
              case None => true
            }
          }).cache()
        },
        investigators = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[EInvestigator](Some(ees.investigators)))).foldLeft(context.sparkSession.emptyDataset[EInvestigator]){
            (left, right) => left.union(right)
          }.cache()
        },
        outcomes = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[EOutcome](Some(ees.outcomes)))).foldLeft(context.sparkSession.emptyDataset[EOutcome]){
            (left, right) => left.union(right)
          }.cache()
        },
        phenotypes = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[EPhenotype](Some(ees.phenotypes)))).foldLeft(context.sparkSession.emptyDataset[EPhenotype]){
            (left, right) => left.union(right)
          }.cache()
        },
        sequencingExperiments = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[ESequencingExperiment](Some(ees.sequencingExperiments)))).foldLeft(context.sparkSession.emptyDataset[ESequencingExperiment]){
            (left, right) => left.union(right)
          }.cache()
        },
        studies = {
          endpoints.map(ees => context.sparkSession.createDataset(retrieval.retrieve[EStudy](Some(ees.studies)))).foldLeft(context.sparkSession.emptyDataset[EStudy]){
            (left, right) => left.union(right)
          }.cache()
        },
        studyFiles = context.sparkSession.emptyDataset[EStudyFile],//context.sparkSession.createDataset(retrieval.retrieve[EStudyFile](Some(endpoints.studyFiles))).cache(),
        graphPath = HPOGraphPath.get(context).cache()
      )
    retrieval.stop()
    dataset
  }
}
