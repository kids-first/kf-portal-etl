package io.kf.etl.processors.common.exceptions

import java.net.URL

object KfExceptions {
  case class DataDumpTargetNotSupportedException(url:URL) extends Exception(s"Can't dump data to ${url.toString}, unsupported protocol '${url.getProtocol}'")
  case class CreateDumpDirectoryFailedException(url:URL) extends Exception(s"Failed to create data dump directory '${url.toString}'")
  case class DataSinkTargetNotSupportedException(url:URL) extends Exception(s"Can't write the downloaded data to ${url.toString}, unsupported protocol '${url.getProtocol}'")
  case class CreateDataSinkDirectoryFailedException(url:URL) extends Exception(s"Failed to create data sink directory '${url.toString}' for DownloadProcessor")

}
