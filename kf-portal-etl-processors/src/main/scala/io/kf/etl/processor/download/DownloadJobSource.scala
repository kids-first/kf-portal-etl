package io.kf.etl.processor.download

import java.net.URL

import io.kf.etl.processor.repo.Repository


class DownloadJobSource(val context: DownloadJobContext) {

  def getRepository(): Repository = {
    Repository(makeURL())
  }

  private def makeURL():URL = {
    ???
  }
}
