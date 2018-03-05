package io.kf.etl.processors.download.output

import java.net.URL

import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.processors.repo.Repository

class DownloadOutput(val context: DownloadContext) {
  def output(placeholder: Unit): Repository = {
    Repository(new URL(context.getJobDataPath()))
  }
}
