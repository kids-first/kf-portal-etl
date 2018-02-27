package io.kf.etl.processor.download.dump

import java.net.URL

import io.kf.etl.processor.download.context.DownloadContext
import io.kf.etl.processor.repo.Repository

class DataSourceDump(val context: DownloadContext) {

  def dump():Repository = {

    PostgresqlDump.dump(context)

    MysqlDump.dump(context)

    Repository(new URL(s"${context.config.dumpPath}"))

  }
}
