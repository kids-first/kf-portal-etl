package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Diagnosis_ES, Participant_ES}
import io.kf.etl.external.dataservice.entity.EDiagnosis
import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeDiagnosis(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {

  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.spark.implicits._

    val enrichedDiagnoses = joinDiagnosisAndOntologyTerms()
      .map {
        case (d, optMondoTerm, optNcitTerm) if optMondoTerm.isDefined => d.copy(diagnosisText = optMondoTerm.map(_.name), mondoIdDiagnosis = formatTerm(optMondoTerm), ncitIdDiagnosis = formatTerm(optNcitTerm))
        case (d, _, optNcitTerm) if optNcitTerm.isDefined => d.copy(diagnosisText = optNcitTerm.map(_.name), mondoIdDiagnosis = None, ncitIdDiagnosis = formatTerm(optNcitTerm))
        case (d, _, _) => d.copy(diagnosisText = d.sourceTextDiagnosis)
      }

    participants
      .joinWith(
        enrichedDiagnoses,
        participants.col("kfId") === enrichedDiagnoses.col("participantId"),
        "left_outer"
      )
      .as[(Participant_ES, Option[EDiagnosis])]
      .groupByKey { case (participant, _) => participant.kfId }
      .mapGroups((_, groupsIterator) => {
        val groups = groupsIterator.toSeq
        val participant = groups.head._1
        val filteredSeq: Seq[Diagnosis_ES] = groups.collect { case (_, Some(d)) => PBEntityConverter.EDiagnosisToDiagnosisES(d) }
        participant.copy(
          diagnoses = filteredSeq
        )
      })
  }

  private def joinDiagnosisAndOntologyTerms(): Dataset[(EDiagnosis, Option[OntologyTerm], Option[OntologyTerm])] = {
    import ctx.entityDataset.diagnoses
    import ctx.entityDataset.ontologyData.{mondoTerms, ncitTerms}
    import ctx.spark.implicits._

    val t: Dataset[(EDiagnosis, Option[OntologyTerm], Option[OntologyTerm])] = diagnoses
      .joinWith(mondoTerms, diagnoses("mondoIdDiagnosis") === mondoTerms("id"), "left_outer")
      .as[(EDiagnosis, Option[OntologyTerm])]
      .joinWith(ncitTerms, $"_1.ncitIdDiagnosis" === ncitTerms("id"), "left_outer")
      .map { case ((d, m), n) => (d, m, Option(n)) }
    t
  }

  def formatTerm(term: Option[OntologyTerm]): Option[String] = term.map(t => s"${t.name} (${t.id})")

}
