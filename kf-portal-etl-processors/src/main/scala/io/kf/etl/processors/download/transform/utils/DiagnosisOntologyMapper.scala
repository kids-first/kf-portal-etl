package io.kf.etl.processors.download.transform.utils

import io.kf.etl.external.dataservice.entity.EDiagnosis
import io.kf.etl.processors.common.ontology.OwlOntologyDataset

case class DiagnosisOntologyMapper() {

  lazy val mondoData = new OwlOntologyDataset("https://s3.amazonaws.com/kf-qa-etl-bucket/ontologies/mondo/mondo.owl")

  def findDiagnosisText(input: EDiagnosis): EDiagnosis = {

    val ontologyText: Option[String] = input.mondoIdDiagnosis match {
      case Some(mondoId) => mondoLookup(mondoId)
      case _ => input.ncitIdDiagnosis match {
        case Some(ncitId) => ncitLookup(ncitId)
        case _ => None
      }
    }

    val diagnosisText = ontologyText match {
      case Some(text) => ontologyText
      case None => input.sourceTextDiagnosis
    }

    input.copy(
      diagnosisText = diagnosisText
    )
  }

  private def mondoLookup (mondoId: String): Option[String] = {
    mondoData.getById(mondoId) match {
      case Some(data) => Some(data.label)
      case _ => None
    }
  }

  private def ncitLookup (ncitId: String): Option[String] = {
    Some(ncitId)
  }


}
