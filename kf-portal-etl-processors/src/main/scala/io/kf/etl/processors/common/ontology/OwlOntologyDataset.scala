package io.kf.etl.processors.common.ontology
import scala.xml.{Elem, Node}

class OwlOntologyDataset(sourceUrl: String) extends OntologyDataset (sourceUrl) {


  def parseClassNode(node: Node): Option[OntologicalClass] = {

    val id = (node \ "id").headOption match {
      case Some(child) => Some(child.text)
      case None => None
    }

    val label = (node \ "label").headOption match {
      case Some(child) => Some(child.text)
      case None => None
    }

    if(id.isDefined && label.isDefined) {
      Some(OntologicalClass(id.get, label.get))
    } else {
      None
    }
  }

  override def buildMapFromSource(sourceXml: Elem): Map[String, OntologicalClass] = {
    sourceXml.child
      .filter(node => node.label.equals("Class"))
      .map(parseClassNode)
      .filter(_.isDefined)
      .map(option => {
        val node = option.get
        (node.id, node)
      }).toMap
  }

}
