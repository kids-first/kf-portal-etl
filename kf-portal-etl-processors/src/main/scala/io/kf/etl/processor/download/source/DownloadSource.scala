package io.kf.etl.processor.download.source

import java.net.URL

import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc


class DownloadSource(val context: DownloadContext) {

  def getRepository(placeholder:Unit): Repository[Doc] = {
    Repository(makeURL())
  }

  private def makeURL():URL = {
    ???
  }
}
