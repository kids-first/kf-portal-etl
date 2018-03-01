package io.kf.etl.processor.document.transform

import io.kf.etl.model.{FileCentric, Participant}
import io.kf.etl.processor.common.ProcessorCommonDefinitions.{DatasetsFromDBTables, ParticipantToGenomicFiles}
import io.kf.etl.processor.document.context.DocumentContext
import io.kf.etl.processor.document.transform.steps._
import io.kf.etl.processor.document.transform.steps.posthandler.{DefaultPostHandler, WriteFileCentricToJsonFile, WriteParticipantsToJsonFile}
import org.apache.spark.sql.Dataset

class DocumentTransformer(val context: DocumentContext) {

  def transform(input: DatasetsFromDBTables): Dataset[FileCentric] = {

    import context.sparkSession.implicits._
    registerSparkTempViewsForPGTables(input)
    val participantToGenomicFiles = mapParticipantAndGenomicFile()

    val ctx = StepContext(context, input, participantToGenomicFiles)

    val (posthandler1, posthandler2) = {
      context.config.write_intermediate_data match {
        case true => ((filename:String) => new WriteParticipantsToJsonFile(ctx, filename), new WriteFileCentricToJsonFile(ctx))
        case false => ((placeholder:String) => new DefaultPostHandler[Dataset[Participant]](), new DefaultPostHandler[Dataset[FileCentric]]())
      }
    }

    Function.chain(
      Seq(
        Step[Dataset[Participant], Dataset[Participant]]("01. merge Study into Participant", new MergeStudy(ctx), posthandler1("step1")),
        Step[Dataset[Participant], Dataset[Participant]]("02. merge Demographic into Participant", new MergeDemographic(ctx), posthandler1("step2")),
        Step[Dataset[Participant], Dataset[Participant]]("03. merge Diagnosis into Participant", new MergeDiagnosis(ctx), posthandler1("step3")),
        Step[Dataset[Participant], Dataset[Participant]]("04. compute HPO reference data and then merge Phenotype into Participant", new MergePhenotype(ctx), posthandler1("step4")),
        Step[Dataset[Participant], Dataset[Participant]]("05. merge 'availableDataTypes' into Participant", new MergeAvailableDataTypesForParticipant(ctx), posthandler1("step5")),
        Step[Dataset[Participant], Dataset[Participant]]("06. merge family member into Participant", new MergeFamilyMember(ctx), posthandler1("step6")),
        Step[Dataset[Participant], Dataset[Participant]]("07. merge Sample, Aliquot into Participant", new MergeSample(ctx), posthandler1("step7"))
      )
    ).andThen(
      Step[Dataset[Participant], Dataset[FileCentric]]("08. build final FileCentric", new BuildFiles(ctx), posthandler2)
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
    }).cache()
  }

}
