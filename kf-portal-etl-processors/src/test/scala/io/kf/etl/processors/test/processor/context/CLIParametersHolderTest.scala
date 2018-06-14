package io.kf.etl.processors.test.processor.context

import io.kf.etl.context.CLIParametersHolder
import io.kf.etl.test.common.KfEtlUnitTestSpec

class CLIParametersHolderTest extends KfEtlUnitTestSpec{
  "A CLIParametersHolder" should "" in {
    val args = Array("-study_id", "sample_id1", "sample_id2", "-index_suffix", "sample_index")

    val holder = new CLIParametersHolder(args)

    holder.study_ids match {
      case Some(ids) => {
        assert(ids.contains("sample_id1"))
        assert(ids.contains("sample_id2"))
      }
      case None => assert(false)
    }

    holder.index_suffix match {
      case Some(index) => {
        println(index)
        assert(index.equals("sample_index"))
      }
      case None => assert(false)
    }
  }
}
