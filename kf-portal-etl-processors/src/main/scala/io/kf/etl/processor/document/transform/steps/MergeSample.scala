package io.kf.etl.processor.document.transform.steps

import io.kf.etl.model.{Aliquot, Participant, Sample}
import io.kf.etl.processor.common.ProcessorCommonDefinitions.{DatasetsFromDBTables, ParticipantToSamples}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

class MergeSample(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]] {
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
    import ctx.parentContext.sparkSession.implicits._
    val samples = buildSample(ctx.dbTables)
    participants.joinWith(samples, col("kfId"), "left").groupByKey(_._1.kfId).mapGroups((parId, iterator) => {
      val list = iterator.toList.filter(_._2!= null)
      list(0)._1.copy(samples = {
        list.flatMap(_._2.samples)
      })
    })
  }

  private def buildSample(all: DatasetsFromDBTables): Dataset[ParticipantToSamples] = {
    import ctx.parentContext.sparkSession.implicits._

    all.sample.joinWith(all.aliquot, all.sample.col("kfId") === all.aliquot.col("sampleId"), "left").groupByKey(_._1.kfId).mapGroups((sampleId, iterator) => {
      val list = iterator.toList
      val tsample = list(0)._1
      (
        tsample.participantId,
        Sample(
          kfId = tsample.kfId,
          uuid = tsample.uuid,
          composition = tsample.composition,
          tissueType = tsample.tissueType,
          tumorDescriptor = tsample.tumorDescriptor,
          anatomicalSite = tsample.anatomicalSite,
          ageAtEventDays = tsample.ageAtEventDays,
          aliquots = {
            list.collect{
              case tuple if(tuple._2 != null) => {
                val taliquot = tuple._2
                Aliquot(
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
              }
            }
          }
        )
      )
    }).groupByKey(_._1).mapGroups((parId, iterator) => {
      ParticipantToSamples(
        parId,
        iterator.map(_._2).toSeq
      )
    })
  }
}
