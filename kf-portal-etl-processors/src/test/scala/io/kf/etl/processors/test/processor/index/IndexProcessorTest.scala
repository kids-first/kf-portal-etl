package io.kf.etl.processors.test.processor.index

import java.io.File
import java.net.URL

import com.typesafe.config.ConfigFactory
import io.kf.etl.common.conf.ESConfig
import io.kf.etl.test.common.KfEtlUnitTestSpec
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.index.context.{IndexConfig, IndexContext}
import io.kf.etl.processors.index.sink.IndexSink
import io.kf.etl.processors.index.source.IndexSource
import io.kf.etl.processors.index.transform.IndexTransformer
import io.kf.etl.processors.repo.Repository
import io.kf.etl.processors.test.common.KfEtlTestEnv
import org.apache.commons.io.FileUtils
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods

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
    val context = new IndexContext(spark, null, "/ttt", IndexConfig(
      in_line_config.getString("name"),
      {
        val esConfig = in_line_config.getConfig("elasticsearch")
        ESConfig(
          esConfig.getString("url"),
          esConfig.getString("index")
        )
      },
      None
    ))
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
      ("file-centric", Repository(new URL(s"file://${tmp}")))
    )

    import org.asynchttpclient.Dsl._
    val client = asyncHttpClient()

    val jvalue =
    (JsonMethods.parse(
      client.prepareGet("http://localhost:9200/index_processor_test/doc/_search").execute().get().getResponseBody
    ) \ "hits" \ "hits")(0) \ "_source"

    jvalue \ "created_datetime" match {
      case JString(value) => assert(value.equals("mock-datetime"))
      case _ => assert(false)
    }

    FileUtils.deleteDirectory(new File(tmp))
    KfEtlTestEnv.elasticsearch.deleteIndices()
    KfEtlTestEnv.elasticsearch.stop()
  }

}
