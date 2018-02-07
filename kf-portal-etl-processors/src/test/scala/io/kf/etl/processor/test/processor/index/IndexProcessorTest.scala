package io.kf.etl.processor.test.processor.index

import java.io.File
import java.net.URL

import com.typesafe.config.ConfigFactory
import io.kf.etl.common.conf.ESConfig
import io.kf.etl.test.common.KfEtlUnitTestSpec
import io.kf.etl.processor.index.IndexProcessor
import io.kf.etl.processor.index.context.IndexContext
import io.kf.etl.processor.index.sink.IndexSink
import io.kf.etl.processor.index.source.IndexSource
import io.kf.etl.processor.index.transform.IndexTransformer
import io.kf.etl.processor.repo.Repository
import io.kf.etl.processor.test.common.KfEtlTestEnv
import org.apache.commons.io.FileUtils

class IndexProcessorTest extends KfEtlUnitTestSpec{

  "A IndexProcessor" should "load data from last step's output and index them into Elasticsearch" in {

    KfEtlTestEnv.elasticsearch.start()

    val tmpdir = System.getProperty("java.io.tmpdir")
    val tmp = (if(tmpdir.charAt(tmpdir.length-1).equals('/')) tmpdir.substring(0, tmpdir.length-1) else tmpdir) + "/kf/datasource"
    println(s"temporary data path is ${tmp}")
    FileUtils.deleteDirectory(new File(tmp))

    val spark = KfEtlTestEnv.spark

    import spark.implicits._
    val ds = spark.createDataset(Seq(KfEtlTestEnv.mock_Doc_Entity))
    ds.write.parquet(tmp)

    val in_line_config = ConfigFactory.parseString(
      """
        {
       name = "index"
       elasticsearch {
       index = "index_processor_test"
       url = "localhost"
       }
      }
      """.stripMargin)

    val esConfig = in_line_config.getConfig("elasticsearch")
    val context = new IndexContext(spark, null, "/ttt", Some(in_line_config))
    val source = new IndexSource(context)
    val transformer = new IndexTransformer(context)
    val sink = new IndexSink(spark,
      ESConfig(
        esConfig.getString("url"),
        esConfig.getString("index")
      )
    )

    val index_processor = new IndexProcessor(context, source.source, transformer.transform, sink.sink)

    index_processor.process(
      Repository(new URL(s"file://${tmp}"))
    )


    FileUtils.deleteDirectory(new File(tmp))
    KfEtlTestEnv.elasticsearch.stop()
  }

}
