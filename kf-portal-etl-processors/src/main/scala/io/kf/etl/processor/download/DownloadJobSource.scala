package io.kf.etl.processor.download

import java.net.URL

import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc


class DownloadJobSource(val context: DownloadJobContext) {

  def getRepository(): Repository[Doc] = {
    Repository(makeURL())
  }

  private def makeURL():URL = {
    ???
  }
}
