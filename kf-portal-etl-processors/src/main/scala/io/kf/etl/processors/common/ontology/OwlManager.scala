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

  private val factory = SAXParserFactory.newInstance

  def getOntologyTermsFromURL(source: URL): Seq[OntologyTerm] = {
    println(s"Start parsing ontology term from $source")
    val handler = new OwlTermHandler(source.toString.contains("ncit"))

    try {
      val parser = factory.newSAXParser
      parser.parse(source.openStream(), handler)

    } catch {
      case e: IOException => e.printStackTrace()
      case e: SAXException => e.printStackTrace()
    }
    println(s"End parsing ontology term from $source")
    handler.getTerms
  }

}

class OwlTermHandler(isNCIT: Boolean) extends DefaultHandler {

  private val terms: ArrayBuffer[OntologyTerm] = ArrayBuffer.empty[OntologyTerm]

  def getTerms:Seq[OntologyTerm] = terms.to[collection.immutable.Seq]

  private var classDepth: Integer = 0
  private var id: Option[String] = None
  private var name: Option[String] = None

  private var sb: Option[StringBuilder] = None

  private val classTag = "owl:Class"
  private val nameTag = "rdfs:label"
  private val idTag = if (isNCIT) "obo:NCIT_NHC0" else "oboInOwl:id"
  private val valueTags = Seq(idTag, nameTag)


  override def startElement(uri: String, localName: String, qName: String, attributes: Attributes): Unit = {
    if (classDepth == 1 && valueTags.contains(qName)) {
      sb = Some(new StringBuilder)
    }

    qName match {
      case `classTag` => classDepth += 1
      case _ =>
    }
  }

  override def endElement(uri: String, localName: String, qName: String): Unit = {
    if (classDepth == 1 && sb.isDefined && valueTags.contains(qName)) {
      qName match {
        case `idTag` => id = if (isNCIT) Some(s"NCIT:${sb.get}") else sb.map(_.toString())
        case `nameTag` => name = sb.map(_.toString())
        case _ =>
      }

      sb = None
    }

    qName match {
      case `classTag` =>
        classDepth -= 1
        if (id.isDefined && name.isDefined) {

          terms += OntologyTerm(
            id = id.get,
            name = name.get
          )
          id = None
          name = None
        }
      case _ =>
    }
  }


  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    sb match {
      case Some(builder) => {
        val value = ch.slice(start, start + length).mkString
        builder.append(value)
      }
      case _ =>
    }
  }
}