package io.kf.etl.processor.download.transform

import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc
import org.apache.spark.sql.Dataset


class DownloadTransformer(val context:DownloadContext) {

  def transform(repo: Repository[Doc]): Dataset[Doc] = {
    repo.load()
  }

}
