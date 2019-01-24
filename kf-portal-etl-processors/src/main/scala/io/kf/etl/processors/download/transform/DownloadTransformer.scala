package io.kf.etl.processors.download.transform

import java.net.URL

import com.trueaccord.scalapb.GeneratedMessageCompanion
import io.kf.etl.external.dataservice.entity._
import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, EntityEndpointSet, OntologiesDataSet}
import io.kf.etl.processors.common.ontology.OwlManager
import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.processors.download.transform.hpo.{HPOGraphPath, HPOTerm}
import io.kf.etl.processors.download.transform.utils.{EntityDataRetriever, EntityParentIDExtractor}

class DownloadTransformer(val context: DownloadContext) {

  val filters = Seq("visible=true")


  def downloadEntities[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]]
  (endpoints: Seq[String], retriever: EntityDataRetriever)
  (implicit
   cmp: GeneratedMessageCompanion[T],
   extractor: EntityParentIDExtractor[T]
  ): Seq[T] = {
    endpoints
      .map( endpoint => {
        retriever.retrieve[T](Some(endpoint))
      })
      .foldLeft(Seq.empty[T]) {
        (left, right) => left.union(right)
      }
  }

  def downloadOntologyData(): OntologiesDataSet = {


    import context.appContext.sparkSession.implicits._
    val spark = context.appContext.sparkSession

    val mondoTerms = OwlManager.getOntologyTermsFromURL(new URL("https://s3.amazonaws.com/kf-qa-etl-bucket/ontologies/mondo/mondo.owl"))
    val ncitTerms = OwlManager.getOntologyTermsFromURL(new URL("https://s3.amazonaws.com/kf-qa-etl-bucket/ontologies/ncit/ncit.owl"))

    OntologiesDataSet(
      hpoGraphPath = HPOGraphPath.get(context).cache,
      hpoTerms   = HPOTerm.get(context).cache,
      mondoTerms = spark.createDataset(mondoTerms),
      ncitTerms  = spark.createDataset(ncitTerms)
    )
  }

  def transform(endpoints: EntityEndpointSet): EntityDataSet = {
    import context.appContext.sparkSession.implicits._
    val spark = context.appContext.sparkSession

    val ontologyData = downloadOntologyData();

    val retriever = EntityDataRetriever(context.config.dataService, filters)

    val participants                     = downloadEntities[EParticipant]                     (endpoints.participants, retriever)
    val families                         = downloadEntities[EFamily]                          (endpoints.families, retriever)
    val biospecimens                     = downloadEntities[EBiospecimen]                     (endpoints.biospecimens, retriever)
    val diagnoses                        = downloadEntities[EDiagnosis]                       (endpoints.diagnoses, retriever)
    val familyRelationships              = downloadEntities[EFamilyRelationship]              (endpoints.familyRelationships, retriever)
    val investigators                    = downloadEntities[EInvestigator]                    (endpoints.investigators, retriever)
    val outcomes                         = downloadEntities[EOutcome]                         (endpoints.outcomes, retriever)
    val phenotypes                       = downloadEntities[EPhenotype]                       (endpoints.phenotypes, retriever)
    val sequencingExperiments            = downloadEntities[ESequencingExperiment]            (endpoints.sequencingExperiments, retriever)
    val sequencingExperimentGenomicFiles = downloadEntities[ESequencingExperimentGenomicFile] (endpoints.sequencingExperimentGenomicFiles, retriever)
    val studies                          = downloadEntities[EStudy]                           (endpoints.studies, retriever)
    val genomicFiles                     = downloadEntities[EGenomicFile]                     (endpoints.genomicFiles, retriever)
    val biospecimenGenomicFiles          = downloadEntities[EBiospecimenGenomicFile]          (endpoints.biospecimenGenomicFiles, retriever)

    val dataset =
      EntityDataSet(
        participants                      = spark.createDataset(participants)                     .cache,
        families                          = spark.createDataset(families)                         .cache,
        biospecimens                      = spark.createDataset(biospecimens)                     .cache,
        familyRelationships               = spark.createDataset(familyRelationships)              .cache,
        investigators                     = spark.createDataset(investigators)                    .cache,
        outcomes                          = spark.createDataset(outcomes)                         .cache,
        phenotypes                        = spark.createDataset(phenotypes)                       .cache,
        sequencingExperiments             = spark.createDataset(sequencingExperiments)            .cache,
        sequencingExperimentGenomicFiles  = spark.createDataset(sequencingExperimentGenomicFiles) .cache,
        studies                           = spark.createDataset(studies)                          .cache,
        biospecimenGenomicFiles           = spark.createDataset(biospecimenGenomicFiles)          .cache,
        diagnoses                         = spark.createDataset(diagnoses)                        .cache,
        genomicFiles                      = spark.createDataset(genomicFiles)
          .filter(_.dataType match {
            case Some(data_type) => {
              !data_type.toLowerCase.split(' ').takeRight(1)(0).equals("index")
            }
            case None => true
          })
          .cache(),
        studyFiles                        = context.appContext.sparkSession.emptyDataset[EStudyFile],

        // following two (graphPath, hpoTerms) are read from HPO mysql db:
        ontologyData            = ontologyData
      )

    retriever.stop()

    //return:
    dataset
  }

}
