package io.kf.etl.processor.download.output

import java.net.URL

import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository

class DownloadOutput(val context: DownloadContext) {
  def output(placeholder: Unit): Repository = {
    Repository(new URL(context.getJobDataPath()))
  }
}
