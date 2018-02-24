package io.kf.etl.processor.document.transform.steps

import io.kf.etl.model.{FileCentric, Participant, SequencingExperiment, Workflow}
import io.kf.etl.processor.common.ProcessorCommonDefinitions.{GenomicFileToParticipants, GenomicFileToSeqExps, GenomicFileToWorkflows}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

class BuildFiles(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[FileCentric]] {
  override def process(participants: Dataset[Participant]): Dataset[FileCentric] = {
    import ctx.parentContext.sparkSession.implicits._
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

    val file2Workflows =
      ctx.dbTables.workflowGenomicFile.joinWith(ctx.dbTables.workflow, ctx.dbTables.workflowGenomicFile.col("workflowId") === ctx.dbTables.workflow.col("kfId")).groupByKey(_._1.genomicFileId).mapGroups((fileId, iterator) => {
        GenomicFileToWorkflows(
          fileId,
          iterator.collect{
            case tuple if(tuple._2 != null) => {
              val tflow = tuple._2
              Workflow(
                kfId = tflow.kfId,
                uuid = tflow.uuid,
                createdAt = tflow.createdAt,
                modifiedAt = tflow.modifiedAt,
                taskId = tflow.taskId,
                name = tflow.name,
                version = tflow.version,
                githubUrl = tflow.githubUrl
              )
            }
          }.toSeq
        )
      })

    val file2Participants =
      ctx.participant2GenomicFiles.joinWith(participants, col("kfId")).flatMap(tuple => {
        tuple._1.fielIds.map(id => (id, tuple._2))
      }).groupByKey(_._1).mapGroups((fileId, iterator) => {
        GenomicFileToParticipants(
          fileId,
          iterator.map(_._2).toSeq
        )
      })

    ctx.dbTables.genomicFile.joinWith(file2SeqExps, col("kfId")).groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {
      val list = iterator.toList

      val tgf = list(0)._1
      FileCentric(
        kfId = tgf.kfId,
        uuid = tgf.uuid,
        createdAt = tgf.createdAt,
        modifiedAt = tgf.modifiedAt,
        fileName = tgf.fileName,
        dataType = tgf.dataType,
        fileFormat = tgf.fileFormat,
        fileUrl = tgf.fileUrl,
        controlledAccess = tgf.controlledAccess,
        md5Sum = tgf.md5Sum,
        sequencingExperiments = iterator.flatMap(_._2.exps).toSeq
      )
    }).joinWith(file2Workflows, col("kfId")).groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {

      val list = iterator.toList
      val fc = list(0)._1

      fc.copy(
        workflow = {
          iterator.flatMap(tuple => {
            tuple._2.flows
          }).toSeq
        }
      )
    }).joinWith(file2Participants, col("kfId")).groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {
      val list = iterator.toList
      val fc = list(0)._1
      fc.copy(
        participants = {
          iterator.flatMap(_._2.participants).toSeq
        }
      )
    })
  }
}
