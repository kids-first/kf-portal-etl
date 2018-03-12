package io.kf.etl.processors.common.step.impl

import io.kf.etl.model.utils.ParticipantToSamples
import io.kf.etl.model.{Aliquot, Participant, Sample}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeSample(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]] {
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {
    import ctx.spark.implicits._
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
  def buildSample(ctx: StepContext): Dataset[ParticipantToSamples] = {
    import ctx.spark.implicits._

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