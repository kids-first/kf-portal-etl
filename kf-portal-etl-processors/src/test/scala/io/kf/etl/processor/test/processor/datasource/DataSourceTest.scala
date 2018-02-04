package io.kf.etl.processor.test.processor.datasource

import io.kf.etl.common.test.common.KfEtlUnitTestSpec
import io.kf.etl.processor.test.common.KfEtlTestEnv
import io.kf.model.Doc
import org.apache.hadoop.fs.Path

class DataSourceTest extends KfEtlUnitTestSpec{
  "The KfHdfsDataProvider and KfHdfsParquetData" should "work together to allow the users to load parquet format data out of HDFS into RDD" in {

    val fs_root = "hdfs://localhost:8020"
    val path = new Path(s"${fs_root}/tmp/kf")

    val spark = KfEtlTestEnv.spark

    val hdfs = KfEtlTestEnv.getHdfs(fs_root)

    hdfs.delete(path, true)
//    hdfs.mkdirs(path)

    import io.kf.etl.processor.datasource.KfHdfsParquetData._
    import spark.implicits._

    val ds = spark.createDataset(Seq(KfEtlTestEnv.fake_Doc_Entity))
    ds.write.parquet(path.toString)


    val loaded = spark.read.kfHdfs(path.toString).as[Doc].cache()
    println(loaded.count())

//    hdfs.delete(path, true)

  }
}
