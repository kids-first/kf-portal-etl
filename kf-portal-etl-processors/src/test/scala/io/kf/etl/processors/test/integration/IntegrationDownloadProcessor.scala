package io.kf.etl.processors.test.integration

import java.io.File
import java.net.URL

import io.kf.etl.processors.common.processor.Processor
import io.kf.etl.processors.download.context.DownloadContext
import io.kf.etl.processors.repo.Repository
import io.kf.test.Doc
import org.apache.commons.io.FileUtils

import scala.util.{Success, Try}

class IntegrationDownloadProcessor(val context: DownloadContext) extends Processor[Unit, Try[Repository]]{
  override def process(input: Unit): Try[Repository] = {

    val tmpdir = System.getProperty("java.io.tmpdir")
    val tmp = (if(tmpdir.charAt(tmpdir.length-1).equals('/')) tmpdir.substring(0, tmpdir.length-1) else tmpdir) + context.config.dataPath.get

    println(s"temporary data path is ${tmp}")
    FileUtils.deleteDirectory(new File(tmp))

    val data = Doc(
      createdDatetime = Some("mock-datetime"),
      dataCategory = "dataCategory",
      dataFormat = "dataFormat",
      dataType = "dataType",
      experimentalStrategy = "experimentalStrategy",
      fileName = "fileName",
      fileSize = 100,
      md5Sum = "md5Sum",
      submitterId = "submitterId"
    )

    import context.sparkSession.implicits._

    context.sparkSession.createDataset(Seq(data)).write.parquet(tmp)

    Success(Repository(new URL(s"file://${tmp}")))
  }
}
