package io.kf.hpo.processors.download.transform

import io.kf.hpo.models.ontology.OntologyTerm

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

object DownloadTransformer {
  val patternId = "id: (HP:[0-9]+)".r
  val patternName = "name: (.*)".r
  val patternIsA = "is_a: (HP:[0-9]+) (.*)".r

  def using[A](r : BufferedSource)(f : BufferedSource => A) : A =
    try {
      f(r)
    } finally {
      r.close()
    }

  def downloadOntologyData(): List[OntologyTerm] = {
    val file = readTextFileWithTry()
    file match {
      case Success(lines) => lines.foldLeft(List.empty[OntologyTerm]){(current, line) =>
        if(line.trim == "[Term]") {
          OntologyTerm("", "") :: current
        } else if(line.matches(patternId.regex)) {
          val patternId(id) = line
          val headOnto = current.head
          headOnto.copy(id = id) :: current.tail
        } else if(line.matches(patternName.regex)) {
          val patternName(name) = line
          val headOnto = current.head
          headOnto.copy(name = name) :: current.tail
        } else if(line.matches(patternIsA.regex)) {
          val patternIsA(id, _) = line
          val headOnto = current.head
          headOnto.copy(parents = headOnto.parents :+ id) :: current.tail
        } else {
          current
        }
      }
      case Failure(_) => List.empty[OntologyTerm] //TODO Log Failure
    }
  }

  def transformOntologyData(data: List[OntologyTerm]): Unit = {
    //TODO add root at the end /required?
    //Use Map
    data.flatMap(term => {
      val cumulativeList =  mutable.MutableList.empty[OntologyTerm]
      getAllParentPath(term, term, data, Nil, cumulativeList)
    }).foreach(println)
  }

  def getAllParentPath(term: OntologyTerm, originalTerm: OntologyTerm, data: List[OntologyTerm], list: Seq[String], cumulativeList: mutable.MutableList[OntologyTerm]): Seq[OntologyTerm] = {
    term.parents.map(p => {
      data.collectFirst { case i if i.id == p => i } match {
        case Some(parentTerm) =>
          if(parentTerm.parents.isEmpty){
            cumulativeList += OntologyTerm(originalTerm.id, originalTerm.name, parents = list :+ p)
          }
          else {
            getAllParentPath(parentTerm, originalTerm, data, list :+ p, cumulativeList)
          }

        case None => //TODO Maybe log terms that parents not found.
      }
    })
    cumulativeList //merge
  }

  def readTextFileWithTry(): Try[List[String]] = {
    Try {
      val lines = using(Source.fromURL("https://raw.githubusercontent.com/obophenotype/human-phenotype-ontology/master/hp.obo")) { source =>
        (for (line <- source.getLines) yield line).toList
      }
      lines
    }
  }

}