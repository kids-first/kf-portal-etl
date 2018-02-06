package io.kf.etl.processor.test.processor.datasource

import java.io.File

import io.kf.etl.common.test.common.KfEtlUnitTestSpec
import io.kf.etl.processor.test.common.KfEtlTestEnv
import io.kf.model.Doc
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{Path => HDFSPath}

class DataSourceTest extends KfEtlUnitTestSpec{
  "The KfHdfsDataProvider and KfHdfsParquetData" should "work together to allow the users to load parquet format data out of HDFS into RDD" in {

//    System.setProperty("HADOOP_USER_NAME", "hdfs")
//    val fs_root = "hdfs://10.30.128.144"
//    val path = new Path(s"${fs_root}/tmp/kf")
//
//    val spark = KfEtlTestEnv.spark
//
//    val hdfs = KfEtlTestEnv.getHdfs(fs_root)
//
//    hdfs.delete(path, true)

    val testDoc = Doc(
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

    Doc.scalaDescriptor.fields.foreach(fd => {
      println(fd.asProto.jsonName)
    })

    val tmpdir = System.getProperty("java.io.tmpdir")
    val tmp = (if(tmpdir.charAt(tmpdir.length-1).equals('/')) tmpdir.substring(0, tmpdir.length-1) else tmpdir) + "/kf/datasource"
    println(s"temporary data path is ${tmp}")
    FileUtils.deleteDirectory(new File(tmp))

    val spark = KfEtlTestEnv.spark
    import io.kf.etl.processor.datasource.KfHdfsParquetData._
    import spark.implicits._
    val ds = spark.createDataset(Seq(KfEtlTestEnv.mock_Doc_Entity))
    ds.write.parquet(tmp)

    val loaded = spark.read.kfHdfs(tmp).as[Doc].cache().collect()
    assert(loaded.size == 1)
    val doc = loaded(0)
    assert(doc.dataFormat.equals("mock-format"))
    assert(doc.fileSize == 100)

    FileUtils.deleteDirectory(new File(tmp))
  }
}
