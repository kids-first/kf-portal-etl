package io.kf.etl.processors.filecentric.transform.steps

import io.kf.etl.model.{FileCentric, Participant, SequencingExperiment}
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.BuildFileCentric.{GenomicFileToParticipants, GenomicFileToSeqExps}
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class BuildFileCentric(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[FileCentric]] {
  override def process(participants: Dataset[Participant]): Dataset[FileCentric] = {
    import ctx.spark.implicits._
    val file2SeqExps =
      ctx.dbTables.genomicFile.joinWith(ctx.dbTables.sequencingExperiment, ctx.dbTables.sequencingExperiment.col("kfId") === ctx.dbTables.genomicFile.col("sequencingExperimentId"), "left").groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {

        GenomicFileToSeqExps(
          fileId,
          iterator.collect{
            case tuple if(tuple._2 != null) => {
              val tseq = tuple._2
              SequencingExperiment(
                kfId = tseq.kfId,
                uuid = tseq.uuid,
                createdAt = tseq.createdAt,
                modifiedAt = tseq.modifiedAt,
                experimentDate = tseq.experimentDate,
                experimentStrategy = tseq.experimentStrategy,
                center = tseq.center,
                libraryName = tseq.libraryName,
                libraryStrand = tseq.libraryStrand,
                isPairedEnd = tseq.isPairedEnd,
                platform = tseq.platform,
                instrumentModel = tseq.instrumentModel,
                maxInsertSize = tseq.maxInsertSize,
                meanInsertSize = tseq.meanInsertSize,
                minInsertSize = tseq.minInsertSize,
                meanDepth = tseq.meanDepth,
                meanReadLength = tseq.meanReadLength,
                totalReads = tseq.totalReads
              )
            }
          }.toSeq
        )
      })

//    val file2Workflows =
//      ctx.dbTables.workflowGenomicFile.joinWith(ctx.dbTables.workflow, ctx.dbTables.workflowGenomicFile.col("workflowId") === ctx.dbTables.workflow.col("kfId")).groupByKey(_._1.genomicFileId).mapGroups((fileId, iterator) => {
//        GenomicFileToWorkflows(
//          fileId,
//          iterator.collect{
//            case tuple if(tuple._2 != null) => {
//              val tflow = tuple._2
//              Workflow(
//                kfId = tflow.kfId,
//                uuid = tflow.uuid,
//                createdAt = tflow.createdAt,
//                modifiedAt = tflow.modifiedAt,
//                taskId = tflow.taskId,
//                name = tflow.name,
//                version = tflow.version,
//                githubUrl = tflow.githubUrl
//              )
//            }
//          }.toSeq
//        )
//      })

    val file2Participants =
      ctx.dbTables.participantGenomicFile.joinWith(participants, ctx.dbTables.participantGenomicFile.col("kfId") === participants.col("kfId")).flatMap(tuple => {
        tuple._1.fileIds.map(id => (id, tuple._2))
      }).groupByKey(_._1).mapGroups((fileId, iterator) => {
        val list = iterator.toList
        GenomicFileToParticipants(
          fileId,
          list.map(_._2).toSeq
        )
      })

    val ds =
      ctx.dbTables.genomicFile.joinWith(file2SeqExps, ctx.dbTables.genomicFile.col("kfId") === file2SeqExps.col("kfId")).groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {
        val list = iterator.toList

        val tgf = list(0)._1
        FileCentric(
          kfId = tgf.kfId,
          uuid = tgf.uuid,
          createdAt = tgf.createdAt,
          modifiedAt = tgf.modifiedAt,
          fileName = tgf.fileName,
          fileSize = tgf.fileSize,
          dataType = tgf.dataType,
          fileFormat = tgf.fileFormat,
          fileUrl = tgf.fileUrl,
          controlledAccess = tgf.controlledAccess,
          md5Sum = tgf.md5Sum,
          sequencingExperiments = list.flatMap(_._2.exps).toSeq
        )
      })
//      .joinWith(file2Workflows, col("kfId")).groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {
//
//      val list = iterator.toList
//      val fc = list(0)._1
//
//      fc.copy(
//        workflow = {
//          iterator.flatMap(tuple => {
//            tuple._2.flows
//          }).toSeq
//        }
//      )
//    })
      ds.joinWith(file2Participants, ds.col("kfId") === file2Participants.col("kfId")).groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {
        val list = iterator.toList
        val fc = list(0)._1
        fc.copy(
          participants = {
            list.flatMap(_._2.participants).toSeq
          }
        )
    })
  }
}

object BuildFileCentric{
  case class GenomicFileToParticipants(kfId:String, participants:Seq[Participant])
  case class GenomicFileToSeqExps(kfId:String, exps: Seq[SequencingExperiment])

}