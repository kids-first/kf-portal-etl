package io.kf.etl.processors.common.ontology

import scala.xml.Elem

abstract class OntologyDataset(sourceUrl: String) {
  import scala.xml.XML

  private val data: Map[String, OntologicalClass] = initializeData()

  def getById(id: String): Option[OntologicalClass] = {
    data.get(id)
  }

  def initializeData(): Map[String, OntologicalClass] = {

    val source = XML.load(sourceUrl)
    buildMapFromSource(source)
  }

  def buildMapFromSource(source: Elem): Map[String, OntologicalClass]
}

case class OntologicalClass(id: String, label: String)