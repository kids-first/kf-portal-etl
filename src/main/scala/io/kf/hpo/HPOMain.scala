package io.kf.hpo

import io.kf.hpo.processors.download.transform.{DownloadTransformer, WriteJson}

object HPOMain extends App {

    val downloadData = DownloadTransformer.downloadOntologyData()

    val allParents = downloadData.flatMap(_.parents.map(_.id))

    val ontologyWithParents = DownloadTransformer.transformOntologyData(downloadData map (i => i.id -> i) toMap)


    val result = ontologyWithParents.map{
        case (k, v) if allParents.contains(k.id) => k -> (v, false)
        case (k, v) => k -> (v, true)
    }

    WriteJson.toJson(result)
}
