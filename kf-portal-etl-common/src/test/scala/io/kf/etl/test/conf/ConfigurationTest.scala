package io.kf.etl.test.conf

import java.net.URL

import com.typesafe.config.ConfigFactory
import io.kf.etl.common.Constants.DEFAULT_CONFIG_FILE_NAME
import io.kf.etl.common.conf.KFConfig
import io.kf.etl.test.common.KfEtlUnitTestSpec

class ConfigurationTest extends KfEtlUnitTestSpec{

  "A KFConfig class" should "load configurations from a source to populate data into its fields" in {
    val config = KFConfig( ConfigFactory.parseURL(new URL(s"classpath:///${DEFAULT_CONFIG_FILE_NAME}")).resolve() )

    assert(config.hdfsConfig.fs.equals("hdfs://10.30.128.144"))
    assert(config.hdfsConfig.root.equals("/tmp/kf"))

    config.processorsConfig.collect{
      case ("document", doc_config) => {
        val datapath = doc_config.getString("data_path")
        assert(datapath.equals("/tmp/kf/document"))
      }
      case ("index", index_config) => {
        println(index_config.getConfig("elasticsearch"))
      }
      case ("download", download_config) => {
        assert(download_config.getString("data_path").equals("file:///tmp/kf"))
      }
    }
  }
}
