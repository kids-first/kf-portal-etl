package io.kf.etl.processors.filecentric.transform.steps

import io.kf.etl.model.utils._
import io.kf.etl.model.{Aliquot, Participant, Sample}
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeSampleAliquot(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[GfId_Participants]]{
  override def process(participants: Dataset[Participant]): Dataset[GfId_Participants] = {

    val all = ctx.dbTables
    import ctx.spark.implicits._

    val aliId_gfId =
      all.genomicFile.joinWith(all.sequencingExperiment, all.genomicFile.col("sequencingExperimentId") === all.sequencingExperiment.col("kfId")).map(tuple => {

        val tgf = tuple._1
        val tse = tuple._2

        AliId_GfId(
          gfId = tgf.kfId,
          aliId = tse.aliquotId
        )
      })

    val sampleId_aliquots_gfId =
      aliId_gfId.joinWith(all.aliquot, aliId_gfId.col("aliId") === all.aliquot.col("kfId")).map(tuple => {
        val taliquot = tuple._2
        SampleId_Aliquot_GfId(
          sampleId = taliquot.sampleId,
          gfId = tuple._1.gfId,
          ali = Aliquot(
            kfId = taliquot.kfId,
            uuid = taliquot.uuid,
            createdAt = taliquot.createdAt,
            modifiedAt = taliquot.modifiedAt,
            shipmentOrigin = taliquot.shipmentOrigin,
            shipmentDestination = taliquot.shipmentDestination,
            shipmentDate = taliquot.shipmentDate,
            analyteType = taliquot.analyteType,
            concentration = taliquot.concentration,
            volume = taliquot.volume
          )
        )
      }).groupByKey(_.sampleId).flatMapGroups((saId, iterator) => {
        val list = iterator.toList

        list.groupBy(_.gfId).map(tuple => {
          SampleId_Aliquots_GfId(
            sampleId = saId,
            gfId = tuple._1,
            aliquots = tuple._2.map(_.ali)
          )
        })
      })


    val parId_samples_gfId =
      sampleId_aliquots_gfId.joinWith(all.sample, sampleId_aliquots_gfId.col("sampleId") === all.sample.col("kfId")).map(tuple => {
        val tsample = tuple._2
        ParticipantId_Sample_GfId(
          parId = tsample.participantId,
          gfId = tuple._1.gfId,
          sample = Sample(
            kfId = tsample.kfId,
            uuid = tsample.uuid,
            composition = tsample.composition,
            tissueType = tsample.tissueType,
            tumorDescriptor = tsample.tumorDescriptor,
            anatomicalSite = tsample.anatomicalSite,
            ageAtEventDays = tsample.ageAtEventDays,
            aliquots = tuple._1.aliquots
          )
        )
      }).groupByKey(_.parId).flatMapGroups((pId, iterator) => {
        val list = iterator.toList

        list.groupBy(_.gfId).map(tuple => {
          ParticipantId_Samples_GfId(
            gfId = tuple._1,
            parId = pId,
            samples = tuple._2.map(_.sample)
          )
        })
      })

    participants.joinWith(parId_samples_gfId, participants.col("kfId") === parId_samples_gfId.col("parId"), "left").filter(_._2 != null).map(tuple => {
      GfId_Participant(
        gfId = tuple._2.gfId,
        participant = tuple._1.copy(
          samples = tuple._2.samples
        )
      )
    }).groupByKey(_.gfId).mapGroups((gf, iterator) => {
      GfId_Participants(gf, iterator.toList.map(_.participant))
    })

  }

}
