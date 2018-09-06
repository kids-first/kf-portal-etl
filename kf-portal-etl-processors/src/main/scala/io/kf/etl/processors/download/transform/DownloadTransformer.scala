package io.kf.etl.processors.download.transform

import com.trueaccord.scalapb.GeneratedMessageCompanion
import io.kf.etl.external.dataservice.entity._
import io.kf.etl.processors.common.ProcessorCommonDefinitions.{EntityDataSet, EntityEndpointSet}
import io.kf.etl.processors.download.context.DownloadContext
import org.apache.spark.sql.Dataset

class DownloadTransformer(val context: DownloadContext) {

  def transform(endpoints: EntityEndpointSet): EntityDataSet = {
    import context.appContext.sparkSession.implicits._

    val filters = Seq("visible=true")
    val retriever = EntityDataRetriever(context.config.dataService, filters)

    def downloadEntities[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]]
      (endpoints: Seq[String])
      (implicit
        cmp: GeneratedMessageCompanion[T],
        extractor: EntityParentIDExtractor[T]
      ): Seq[T] =
    {
      endpoints.map(endpoint => {
          retriever.retrieve[T](Some(endpoint))
      }).foldLeft(Seq.empty[T]) {
        (left, right) => left.union(right)
      }
    }

    val participants            = downloadEntities[EParticipant]            (endpoints.participants)
    val families                = downloadEntities[EFamily]                 (endpoints.families)
    val biospecimens            = downloadEntities[EBiospecimen]            (endpoints.biospecimens)
    val diagnoses               = downloadEntities[EDiagnosis]              (endpoints.diagnoses)
    val familyRelationships     = downloadEntities[EFamilyRelationship]     (endpoints.familyRelationships)
    val investigators           = downloadEntities[EInvestigator]           (endpoints.investigators)
    val outcomes                = downloadEntities[EOutcome]                (endpoints.outcomes)
    val phenotypes              = downloadEntities[EPhenotype]              (endpoints.phenotypes)
    val sequencingExperiments   = downloadEntities[ESequencingExperiment]   (endpoints.sequencingExperiments)
    val studies                 = downloadEntities[EStudy]                  (endpoints.studies)
    val genomicFiles            = downloadEntities[EGenomicFile]            (endpoints.genomicFiles)
    val biospecimenGenomicFiles = downloadEntities[EBiospecimenGenomicFile] (endpoints.biospecimenGenomicFiles)


    val spark =context.appContext.sparkSession
    val dataset =
      EntityDataSet(
        participants            = spark.createDataset(participants)           .cache,
        families                = spark.createDataset(families)               .cache,
        biospecimens            = spark.createDataset(biospecimens)           .cache,
        diagnoses               = spark.createDataset(diagnoses)              .cache,
        familyRelationships     = spark.createDataset(familyRelationships)    .cache,
        investigators           = spark.createDataset(investigators)          .cache,
        outcomes                = spark.createDataset(outcomes)               .cache,
        phenotypes              = spark.createDataset(phenotypes)             .cache,
        sequencingExperiments   = spark.createDataset(sequencingExperiments)  .cache,
        studies                 = spark.createDataset(studies)                .cache,
        biospecimenGenomicFiles = spark.createDataset(biospecimenGenomicFiles).cache,
        genomicFiles            = spark.createDataset(genomicFiles)
                                  .filter(_.dataType match {
                                      case Some(data_type) => {
                                        !data_type.toLowerCase.split(' ').takeRight(1)(0).equals("index")
                                      }
                                      case None => true
                                    }
                                  )
                                  .cache(),
        studyFiles              = context.appContext.sparkSession.emptyDataset[EStudyFile],
        graphPath               = HPOGraphPath.get(context).cache
      )

    retriever.stop()

    //return:
    dataset
  }

}
