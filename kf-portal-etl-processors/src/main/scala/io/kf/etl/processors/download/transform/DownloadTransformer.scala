package io.kf.etl.processors.download.transform

import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, EntityEndpointSet}
import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.external.dataservice.entity._

class DownloadTransformer(val context:DownloadContext) {

  def transform(endpoints: Seq[EntityEndpointSet]): EntityDataSet = {

    import context.appContext.sparkSession.implicits._
    import EntityParentIDExtractor._
    val retrieval = EntityDataRetrieval(context.config.dataService.url)

    val dataset =
      EntityDataSet(
        participants = {
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[EParticipant](Some(ees.participants), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[EParticipant]){
            (left, right) => left.union(right)
          }.cache()
        },
        families = {
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[EFamily](Some(ees.families), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[EFamily]){
            (left, right) => left.union(right)
          }.cache()
        },
        biospecimens = {
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[EBiospecimen](Some(ees.biospecimens), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[EBiospecimen]){
            (left, right) => left.union(right)
          }.cache()
        },
        diagnoses = {
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[EDiagnosis](Some(ees.diagnoses), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[EDiagnosis]){
            (left, right) => left.union(right)
          }.cache()
        },
        familyRelationships = {
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[EFamilyRelationship](Some(ees.familyRelationships), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[EFamilyRelationship]){
            (left, right) => left.union(right)
          }.cache()
        },
        genomicFiles = {
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[EGenomicFile](Some(ees.genomicFiles), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[EGenomicFile]){
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
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[EInvestigator](Some(ees.investigators), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[EInvestigator]){
            (left, right) => left.union(right)
          }.cache()
        },
        outcomes = {
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[EOutcome](Some(ees.outcomes), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[EOutcome]){
            (left, right) => left.union(right)
          }.cache()
        },
        phenotypes = {
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[EPhenotype](Some(ees.phenotypes), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[EPhenotype]){
            (left, right) => left.union(right)
          }.cache()
        },
        sequencingExperiments = {
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[ESequencingExperiment](Some(ees.sequencingExperiments), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[ESequencingExperiment]){
            (left, right) => left.union(right)
          }.cache()
        },
        studies = {
          endpoints.map(ees => context.appContext.sparkSession.createDataset(retrieval.retrieve1[EStudy](Some(ees.studies), Seq.empty))).foldLeft(context.appContext.sparkSession.emptyDataset[EStudy]){
            (left, right) => left.union(right)
          }.cache()
        },
        studyFiles = context.appContext.sparkSession.emptyDataset[EStudyFile],//context.appContext.sparkSession.createDataset(retrieval.retrieve[EStudyFile](Some(endpoints.studyFiles))).cache(),
        graphPath = HPOGraphPath.get(context).cache()
      )
    retrieval.stop()
    dataset
  }
}
