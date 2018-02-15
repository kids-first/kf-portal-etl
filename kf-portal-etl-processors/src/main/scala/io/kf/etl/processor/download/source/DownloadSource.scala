package io.kf.etl.processor.download.source

import java.net.URL

import io.kf.etl.processor.download.DataSourceDump
import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository


class DownloadSource(val context: DownloadContext) {

  def getRepository(placeholder:Unit): Repository = {

    val dumper = new DataSourceDump(context)
    dumper.dump()

    Repository(new URL(context.config.dumpPath))
  }

}
