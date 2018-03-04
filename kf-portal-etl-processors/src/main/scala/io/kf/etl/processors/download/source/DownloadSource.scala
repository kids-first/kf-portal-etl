package io.kf.etl.processors.download.source

import java.net.URL

import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.processors.download.dump.DataSourceDump
import io.kf.etl.processors.repo.Repository


class DownloadSource(val context: DownloadContext) {

  def getRepository(placeholder:Unit): Repository = {

    val dumper = new DataSourceDump(context)
    dumper.dump()

    Repository(new URL(context.config.dumpPath))
  }

}
