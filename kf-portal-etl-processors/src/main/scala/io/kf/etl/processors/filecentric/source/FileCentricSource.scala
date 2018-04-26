package io.kf.etl.processors.filecentric.source

import io.kf.etl.model.utils.{BiospecimenId_GenomicFileId, GenomicFileId_ParticipantId, ParticipantId_BiospecimenId}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.filecentric.context.FileCentricContext
import org.apache.spark.sql.Dataset

class FileCentricSource(val context: FileCentricContext) {
  def source(data: EntityDataSet): EntityDataSet = {
    data
  }

  def mapParticipantAndGenomicFile(data: EntityDataSet): Dataset[GenomicFileId_ParticipantId] = {

    import context.sparkSession.implicits._
    val par_bio =
      data.participants.flatMap(participant => {
        participant.biospecimens.map(bio => ParticipantId_BiospecimenId(parId = participant.kfId.get, bioId = bio))
      })

    val bio_gf =
      data.biospecimens.flatMap(bio => {
        bio.genomicFiles.map(gf => BiospecimenId_GenomicFileId(bioId = bio.kfId.get, gfId = gf))
      })

    bio_gf.joinWith(par_bio, bio_gf.col("bioId") === par_bio.col("bioId")).map(tuple => {
      GenomicFileId_ParticipantId(fileId = tuple._1.gfId, parId = tuple._2.parId)
    }).cache()
  }
}
