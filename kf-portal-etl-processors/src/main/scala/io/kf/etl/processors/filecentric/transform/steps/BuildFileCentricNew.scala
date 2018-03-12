package io.kf.etl.processors.filecentric.transform.steps

import io.kf.etl.model.utils.GfId_Participants
import io.kf.etl.model.{FileCentric, SequencingExperiment}
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.BuildFileCentric.GenomicFileToSeqExps
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class BuildFileCentricNew(override val ctx:StepContext)  extends StepExecutable[Dataset[GfId_Participants], Dataset[FileCentric]] {
  override def process(input: Dataset[GfId_Participants]): Dataset[FileCentric] = {
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

    val filecentric_with_SequencingExp =
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
          sequencingExperiments = list.flatMap(_._2.exps)
        )
      })

    filecentric_with_SequencingExp.joinWith(input, filecentric_with_SequencingExp.col("kfId") === input.col("gfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case None => tuple._1
        case Some(participants) => {
          tuple._1.copy(
            participants = tuple._2.participants
          )
        }
      }
    })
  }
}
