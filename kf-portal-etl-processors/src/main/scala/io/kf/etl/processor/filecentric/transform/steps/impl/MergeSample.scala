package io.kf.etl.processor.filecentric.transform.steps.impl

import io.kf.etl.model.filecentric.{Aliquot, Participant, Sample}
import io.kf.etl.processor.common.ProcessorCommonDefinitions.{DatasetsFromDBTables, ParticipantToSamples}
import io.kf.etl.processor.filecentric.transform.steps.StepExecutable
import io.kf.etl.processor.filecentric.transform.steps.context.FileCentricStepContext
import org.apache.spark.sql.Dataset

class MergeSample(override val ctx:FileCentricStepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]] {
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
    import ctx.parentContext.sparkSession.implicits._
    val samples = MergeSampleHelper.buildSample(ctx)
    participants.joinWith(samples, participants.col("kfId") === samples.col("kfId"), "left").groupByKey(_._1.kfId).mapGroups((parId, iterator) => {
      val list = iterator.toList
      val filteredList = list.filter(_._2!= null)
      if(filteredList.size == 0) {
        list(0)._1
      }
      else {
        list(0)._1.copy(samples = {
          filteredList.flatMap(_._2.samples)
        })
      }

    })
  }

}

// helper class is defined for avoiding to make MergeSample serializable
object MergeSampleHelper {
  def buildSample(ctx: FileCentricStepContext): Dataset[ParticipantToSamples] = {
    import ctx.parentContext.sparkSession.implicits._

    val all: DatasetsFromDBTables = ctx.dbTables
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
