package io.kf.etl.processor.document.transform.steps

import io.kf.etl.model.{FileCentric, Participant}
import io.kf.etl.processor.common.ProcessorCommonDefinitions.{DatasetsFromDBTables, ParticipantToGenomicFiles}
import io.kf.etl.processor.document.context.DocumentContext
import org.apache.spark.sql.Dataset

class DocumentTransformerNew(val context: DocumentContext) {

  def transform(input: DatasetsFromDBTables): Dataset[FileCentric] = {

    import context.sparkSession.implicits._
    registerSparkTempViewsForPGTables(input)
    val participantToGenomicFiles = mapParticipantAndGenomicFile()

    val ctx = StepContext(context, input, participantToGenomicFiles)

    Function.chain(
      Seq(
        Step[Dataset[Participant], Dataset[Participant]]("01. merge Demographic into Participant", new MergeDemographicToParticipant(ctx)),
        Step[Dataset[Participant], Dataset[Participant]]("02. merge Diagnosis into Participant", new MergeDiagnosis(ctx)),
        Step[Dataset[Participant], Dataset[Participant]]("03. compute HPO reference data and then merge Phenotype into Participant", new MergePhenotype(ctx)),
        Step[Dataset[Participant], Dataset[Participant]]("04. merge Study into Participant", new MergeStudy(ctx)),
        Step[Dataset[Participant], Dataset[Participant]]("05. merge 'availableDataTypes' into Participant", new MergeAvailableDataTypesForParticipant(ctx)),
        Step[Dataset[Participant], Dataset[Participant]]("06. merge family member into Participant", new MergeFamilyMember(ctx)),
        Step[Dataset[Participant], Dataset[Participant]]("07. merge Sample, Aliquot into Participant", new MergeSample(ctx))
      )
    ).andThen(
      Step[Dataset[Participant], Dataset[FileCentric]]("08. build final FileCentric", new BuildFiles(ctx))
    )(context.sparkSession.emptyDataset[Participant])

  }

  def registerSparkTempViewsForPGTables(all: DatasetsFromDBTables) = {
    all.study.createOrReplaceTempView("ST")
    all.participant.createOrReplaceTempView("PAR")
    all.demographic.createOrReplaceTempView("DG")
    all.sample.createOrReplaceTempView("SA")
    all.aliquot.createOrReplaceTempView("AL")
    all.sequencingExperiment.createOrReplaceTempView("SE")
    all.diagnosis.createOrReplaceTempView("DI")
    all.phenotype.createOrReplaceTempView("PT")
//    all.outcome.createOrReplaceTempView("OC")
    all.genomicFile.createOrReplaceTempView("GF")
//    all.workflow.createOrReplaceTempView("WF")
    all.familyRelationship.createOrReplaceTempView("FR")
//    all.workflowGenomicFile.createOrReplaceTempView("WG")
  }

  def mapParticipantAndGenomicFile(): Dataset[ParticipantToGenomicFiles] = {
    val sql =
      """
         select PAR.kfId, AA.gfId, AA.dataType from PAR left join
           (select SA.participantId as kfId, BB.gfId, BB.dataType from SA left join
             (select AL.sampleId as kfId, CC.gfId, CC.dataType from AL left join
                (select SE.aliquotId as kfId, GF.kfId as gfId, GF.dataType from SE left join GF on SE.kfId = GF.sequencingExperimentId) as CC
               on AL.kfId = CC.kfId) as BB
             on SA.kfId = BB.kfId ) as AA
          on PAR.kfId = AA.kfId
      """.stripMargin

    import context.sparkSession.implicits._
    context.sparkSession.sql(sql).groupByKey(_.getString(0)).mapGroups((par_id, iterator) => {

      val list = iterator.toList

      ParticipantToGenomicFiles(
        par_id,
        list.collect{
          case row if(row.getString(1) != null) => row.getString(1)
        },
        list.collect{
          case row if(row.getString(2) != null) => row.getString(1)
        }
      )
    })
  }

}
