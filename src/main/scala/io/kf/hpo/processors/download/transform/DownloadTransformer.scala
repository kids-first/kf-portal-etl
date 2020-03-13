package io.kf.hpo.processors.download.transform

import io.kf.hpo.models.ontology.OntologyTerm

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

object DownloadTransformer {
  val patternId = "id: ([A-Z]+:[0-9]+)".r
  val patternName = "name: (.*)".r
  val patternIsA = "is_a: ([A-Z]+:[0-9]+) (\\{.*})? ?! (.*)".r

  def using[A](r : BufferedSource)(f : BufferedSource => A) : A =
    try {
      f(r)
    }
    finally {
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
        }
        else if(line.matches(patternIsA.regex)) {
          val patternIsA(id,_ , name) = line
          val headOnto = current.head
          val headOntoCopy = headOnto.copy(parents = headOnto.parents :+ OntologyTerm(id, name, Nil))
          headOntoCopy :: current.tail
        }
        else {
          current
        }
      }
      case Failure(_) => List.empty[OntologyTerm] //TODO Log Failure
    }
  }

  def addParentsToAncestors(map: Map[String, OntologyTerm]): Map[String, OntologyTerm] = {
    map.mapValues(v => v.copy(parents = addParents(v.parents, map)))
  }

  def addParents(seqOntologyTerm: Seq[OntologyTerm], map: Map[String, OntologyTerm]): Seq[OntologyTerm] = {
    seqOntologyTerm.map(t => t.copy(parents = map(t.id).parents))
  }

  def transformOntologyData(data: Map[String, OntologyTerm]) = {
    val allParents = data.values.flatMap(_.parents.map(_.id)).toSet
    data.flatMap(term => {
      val cumulativeList =  mutable.Map.empty[OntologyTerm, Set[OntologyTerm]]
      getAllParentPath(term._2, term._2, data, Set.empty[OntologyTerm], cumulativeList, allParents)
    })
  }

  def getAllParentPath(term: OntologyTerm, originalTerm: OntologyTerm, data: Map[String, OntologyTerm], list: Set[OntologyTerm], cumulativeList: mutable.Map[OntologyTerm, Set[OntologyTerm]], allParents: Set[String]): mutable.Map[OntologyTerm, Set[OntologyTerm]] = {
    term.parents.foreach(p => {
      val parentTerm = data(p.id)

      if(parentTerm.parents.isEmpty){
        cumulativeList.get(originalTerm) match {
          case Some(value) => cumulativeList.update(originalTerm, value ++ list + p)
          case None => cumulativeList.update(originalTerm, list + p)
        }
      }
      else {
        getAllParentPath(parentTerm, originalTerm, data, list + p, cumulativeList, allParents)
      }
    })
    cumulativeList
  }

  def readTextFileWithTry(): Try[List[String]] = {
    Try {
      val lines = using(Source.fromURL("https://raw.githubusercontent.com/obophenotype/human-phenotype-ontology/master/hp.obo")) { source =>
//      val lines = using(Source.fromURL("file:///home/adrianpaul/Downloads/mondo.obo")) { source =>
        (for (line <- source.getLines) yield line).toList
      }
      lines
    }
  }

}