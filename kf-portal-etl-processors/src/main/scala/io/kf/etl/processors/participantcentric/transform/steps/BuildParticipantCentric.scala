package io.kf.etl.processors.participantcentric.transform.steps

import io.kf.etl.model.utils.{GenomicFileId_ParticipantId, ParticipantIdToFiles}
import io.kf.etl.model.{File, Participant, ParticipantCentric, SequencingExperiment}
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class BuildParticipantCentric(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[ParticipantCentric]] {
  override def process(participants: Dataset[Participant]): Dataset[ParticipantCentric] = {

    import ctx.spark.implicits._

    val fileWithSE =
      ctx.dbTables.genomicFile.joinWith(ctx.dbTables.sequencingExperiment, ctx.dbTables.sequencingExperiment.col("kfId") === ctx.dbTables.genomicFile.col("sequencingExperimentId"), "left").groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {

        val list = iterator.toList
        val filteredList = list.filter(_._2 != null)
        val tFile = list(0)._1

        File(
          kfId = tFile.kfId,
          uuid = tFile.uuid,
          createdAt = tFile.createdAt,
          modifiedAt = tFile.modifiedAt,
          controlledAccess = tFile.controlledAccess,
          fileFormat = tFile.fileFormat,
          fileSize = tFile.fileSize,
          dataType = tFile.dataType,
          fileName = tFile.fileName,
          fileUrl = tFile.fileUrl,
          md5Sum = tFile.md5Sum,
          sequencingExperiments = list.collect{
            case tuple => {
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
          }
        )
      })

    val all = ctx.dbTables

    val gf_par =
      all.participantGenomicFile.flatMap(pgf => {
        pgf.fileIds.map(fileId => GenomicFileId_ParticipantId(fileId, pgf.kfId))
      })

    val parId2Files =
      gf_par.joinWith(fileWithSE, gf_par.col("fileId") === fileWithSE.col("kfId")).groupByKey(_._1.parId).mapGroups((parId, iterator) => {
        ParticipantIdToFiles(
          parId,
          iterator.toList.map(_._2)
        )
      })

    participants.joinWith(parId2Files, participants.col("kfId") === parId2Files.col("parId"), "left").groupByKey(_._1.kfId).mapGroups((parid, iterator) => {
      val list = iterator.toList
      val filteredList = list.filter(_._2 != null)

      val par = list(0)._1
      ParticipantCentric(
        kfId = par.kfId,
        uuid = par.uuid,
        createdAt = par.createdAt,
        modifiedAt = par.modifiedAt,
        availableDataTypes = par.availableDataTypes,
        consentType = par.consentType,
        ethnicity = par.ethnicity,
        diagnoses = par.diagnoses,
        externalId = par.externalId,
        family = par.family,
        gender = par.gender,
        isProband = par.isProband,
        phenotype = par.phenotype,
        race = par.race,
        samples = par.samples,
        study = par.study,
        files = {
          filteredList.size match {
            case 0 => Seq.empty[File]
            case _ => {
              filteredList.flatMap(_._2.files)
            }
          }
        }
      )

    })
  }
}
