package io.kf.hpo

import io.kf.hpo.models.ontology.OntologyTerm
import io.kf.hpo.processors.download.transform.{DownloadTransformer, WriteJson}

object HPOMain extends App {

    val dT: Seq[OntologyTerm] = DownloadTransformer.downloadOntologyData()

    val mapDT = dT map(d => d.id -> d) toMap

    val dTwAncestorsParents = DownloadTransformer.addParentsToAncestors(mapDT)

    val allParents = dT.flatMap(_.parents.map(_.id))

    val ontologyWithParents = DownloadTransformer.transformOntologyData(dTwAncestorsParents)

    val result = ontologyWithParents.map{
        case (k, v) if allParents.contains(k.id) => k -> (v, false)
        case (k, v) => k -> (v, true)
    }

    WriteJson.toJson(result)
}
