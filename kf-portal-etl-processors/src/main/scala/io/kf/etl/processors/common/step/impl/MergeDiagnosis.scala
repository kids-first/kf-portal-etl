package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Diagnosis_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EDiagnosis
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset

class MergeDiagnosis(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {

  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.spark.implicits._

    val mondo = generateMondoTerms()
    val ncit = generateNcitTerms()

    val diagnoses = ctx.entityDataset.diagnoses
      .map(diagnosis => {
        val ontologyText: Option[String] = diagnosis.mondoIdDiagnosis match {
          case Some(mondoId) => mondo.value.get(mondoId)
          case _ => diagnosis.ncitIdDiagnosis match {
            case Some(ncitId) => ncit.value.get(ncitId)
            case _ => None
          }
        }

        val text = ontologyText match {
          case Some(_) => ontologyText
          case None => diagnosis.sourceTextDiagnosis
        }

        diagnosis.copy(
          diagnosisText= text
        )
      }
      )

    participants
      .joinWith(
        diagnoses,
        participants.col("kfId") === diagnoses.col("participantId"),
        "left_outer"
      )
      .groupByKey( tuple => {
        tuple._1.kfId.get
      })
      .mapGroups( (_, iterator) => {
        val seq = iterator.toSeq

        val participant = seq(0)._1

        val filteredSeq = seq.filter(_._2 != null)

        filteredSeq.size match {
          case 0 => participant
          case _ => {
            participant.copy(
              diagnoses = seq.map(tuple => PBEntityConverter.EDiagnosisToDiagnosisES(tuple._2))
            )
          }
        }

      })
  }

  def generateMondoTerms(): Broadcast[Map[String, String]] = {
    ctx.spark.sparkContext.broadcast(
      ctx.entityDataset.ontologyData.mondoTerms.collect.map(term=>(term.id, term.name)).toMap
    )
  }

  def generateNcitTerms(): Broadcast[Map[String, String]] = {
    ctx.spark.sparkContext.broadcast(
      ctx.entityDataset.ontologyData.ncitTerms.collect.map(term=>(term.id, term.name)).toMap
    )
  }

}
