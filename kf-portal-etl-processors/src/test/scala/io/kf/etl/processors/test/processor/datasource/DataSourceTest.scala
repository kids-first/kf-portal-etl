package io.kf.etl.processors.test.processor.datasource

import java.io.File

import io.kf.etl.test.common.KfEtlUnitTestSpec
import io.kf.etl.processors.test.common.KfEtlTestEnv
import io.kf.test.Doc
import org.apache.commons.io.FileUtils

class DataSourceTest extends KfEtlUnitTestSpec{
  "The KfHdfsDataProvider and KfHdfsParquetData" should "work together to allow the users to load parquet format data out of HDFS into RDD" in {

    Doc.scalaDescriptor.fields.foreach(fd => {
      println(fd.asProto.jsonName)
    })

    val tmpdir = System.getProperty("java.io.tmpdir")
    val tmp = (if(tmpdir.charAt(tmpdir.length-1).equals('/')) tmpdir.substring(0, tmpdir.length-1) else tmpdir) + "/kf/datasource"
    println(s"temporary data path is ${tmp}")
    FileUtils.deleteDirectory(new File(tmp))

    val spark = KfEtlTestEnv.spark
    import io.kf.etl.processors.datasource.KfHdfsParquetData._
    import spark.implicits._
    val ds = spark.createDataset(Seq(KfEtlTestEnv.mock_Doc_Entity))
    ds.write.parquet(tmp)

    val loaded = spark.read.kfHdfsParquet(tmp).as[Doc].cache().collect()
    assert(loaded.size == 1)
    val doc = loaded(0)
    assert(doc.dataFormat.equals("dataFormat"))
    assert(doc.fileSize == 100)

    FileUtils.deleteDirectory(new File(tmp))
  }
}
