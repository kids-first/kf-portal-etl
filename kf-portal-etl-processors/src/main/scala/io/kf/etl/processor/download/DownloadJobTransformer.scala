package io.kf.etl.processor.download

import java.net.URL

import com.google.gson.GsonBuilder
import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}


class DownloadJobTransformer(val context:DownloadJobContext) {

  def transform(repo: Repository[Doc]): Dataset[Doc] = {
    repo.load()
  }

}
