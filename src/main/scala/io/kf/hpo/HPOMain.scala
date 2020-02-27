package io.kf.hpo

import io.kf.hpo.processors.download.transform.DownloadTransformer

object HPOMain extends App {

    val downloadData = DownloadTransformer.downloadOntologyData()

    val ontologyWithParents = DownloadTransformer.transformOntologyData(downloadData map (i => i.id -> i) toMap)


}
