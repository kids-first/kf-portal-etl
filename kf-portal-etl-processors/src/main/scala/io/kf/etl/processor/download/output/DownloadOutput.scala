package io.kf.etl.processor.download.output

import java.net.URL

import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository
import io.kf.model.Doc

import scala.util.Try

class DownloadOutput(val context: DownloadContext) {
  def output(placeholder: Unit): Try[Repository] = {
    Try(Repository(new URL(context.getJobDataPath())))
  }
}
