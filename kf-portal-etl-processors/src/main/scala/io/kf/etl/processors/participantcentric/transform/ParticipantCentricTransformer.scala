package io.kf.etl.processors.participantcentric.transform

import io.kf.etl.model.ParticipantCentric
import io.kf.etl.processors.common.ProcessorCommonDefinitions.{DatasetsFromDBTables, ParticipantToGenomicFiles}
import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext
import org.apache.spark.sql.Dataset

class ParticipantCentricTransformer(val context: ParticipantCentricContext) {
  def transform(input: DatasetsFromDBTables): Dataset[ParticipantCentric] = ???

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
          case row if(row.getString(2) != null) => row.getString(2)
        }
      )
    }).cache()
  }
}
