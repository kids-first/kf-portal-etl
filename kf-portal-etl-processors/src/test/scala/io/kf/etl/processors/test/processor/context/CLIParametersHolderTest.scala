package io.kf.etl.processors.test.processor.context

import java.net.URL

import io.kf.etl.context.CLIParametersHolder
import io.kf.etl.test.common.KfEtlUnitTestSpec

class CLIParametersHolderTest extends KfEtlUnitTestSpec{
  "A CLIParametersHolder" should "" in {
    val args = Array("-study_id", "sample_id1", "sample_id2", "-release_id", "sample_index")

    val holder = new CLIParametersHolder(args)

    holder.study_ids match {
      case Some(ids) => {
        assert(ids.contains("sample_id1"))
        assert(ids.contains("sample_id2"))
      }
      case None => assert(false)
    }

    holder.release_id match {
      case Some(index) => {
        println(index)
        assert(index.equals("sample_index"))
      }
      case None => assert(false)
    }
  }

  "CLIParameterHolder" should "parse -study_id_file" in {
    val args = Array("-study_id_file", "classpath:/study_ids.txt", "-study_id", "123", "456")

    val holder = new CLIParametersHolder(args)
    holder.study_ids match {
      case Some(ids) => {
        assert(ids.contains("123"))
        assert(ids.contains("hilkjlaksjdl"))
      }
      case None => assert(false)
    }
  }

  "s3 url" should "" in {
    val url = new URL("s3://kf-dev-etl-bucket/study_ids.txt")
    println(url.getProtocol)
    println(url.getHost)
    println(url.getPath)

  }

  "CLIParameterHolder-S3" should "parse -study_id_file s3..." in {

    val args = Array("-study_id_file", "s3://kf-dev-etl-bucket/study_ids.txt", "-study_id", "123", "456")

    val holder = new CLIParametersHolder(args)
    holder.study_ids match {
      case Some(ids) => {
        assert(ids.contains("123"))
        assert(ids.contains("hilkjlaksjdl"))
      }
      case None => assert(false)
    }
  }

}
