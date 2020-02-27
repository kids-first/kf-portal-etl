package io.kf.hpo

import io.kf.hpo.models.ontology.OntologyTerm
import io.kf.hpo.processors.download.transform.DownloadTransformer
import org.apache.spark.sql.SparkSession

object HPOMain extends App {
    val downloadData = DownloadTransformer.downloadOntologyData()
    DownloadTransformer.transformOntologyData(downloadData)
}
