package io.kf.etl.processors.common.ontology

import java.io.IOException
import java.net.URL

import io.kf.etl.external.hpo.OntologyTerm
import javax.xml.parsers.SAXParserFactory
import org.xml.sax.Attributes
import org.xml.sax.helpers.DefaultHandler

import scala.collection.mutable.ArrayBuffer
import scala.xml.SAXException

object OwlManager {

  val factory = SAXParserFactory.newInstance

  def getOntologyTermsFromURL(source: URL): Seq[OntologyTerm] = {

    val handler = new OwlTermHandler()

    try {
      val parser = factory.newSAXParser
      parser.parse(source.openStream(), handler)

    } catch {
      case e: IOException => e.printStackTrace()
      case e: SAXException => e.printStackTrace()
    }

    handler.getTerms
  }

}

class OwlTermHandler extends DefaultHandler {

  private val terms = ArrayBuffer.empty[OntologyTerm]
  private var count: Int = 0

  private var inClass:Boolean = false
  private var id: Option[String] = None
  private var name: Option[String] = None

  private var sb:Option[StringBuilder] = None

  private val classTag = "owl:Class"
  private val idTag = "oboInOwl:id"
  private val nameTag = "rdfs:label"
  private val valueTags = Seq(idTag, nameTag)

  def getTerms(): Seq[OntologyTerm] = {
    terms
  }

  override def startElement(uri: String, localName: String, qName: String, attributes: Attributes): Unit = {
    if (inClass && valueTags.contains(qName)) {
      sb = Some(new StringBuilder)
    }

    qName match {
      case `classTag` => inClass = true
      case _ =>
    }
  }

  override def endElement(uri: String, localName: String, qName: String): Unit = {
    if (inClass && sb.isDefined && valueTags.contains(qName)) {
      qName match {
        case `idTag` => id = Some(sb.get.toString)
        case `nameTag` => name = Some(sb.get.toString)
      }

      sb = None
    }

    qName match {
      case `classTag` => {
        inClass = false
        if (id.isDefined && name.isDefined) {
          terms += OntologyTerm(
            id = id.get,
            name = name.get
          )
          id = None
          name = None
        }
      }
      case _ =>
    }
  }


  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    sb match {
      case Some(builder) => {
        val value = ch.slice(start, start+length).mkString
        builder.append(value)
      }
      case _ =>
    }
  }
}