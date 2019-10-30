package io.kf.etl.processors.context

import io.kf.etl.context.CLIParametersHolder
import org.scalatest._

class CLIParametersHolderTest extends FlatSpec with Matchers with OptionValues with Inside with Inspectors {
  "A CLIParametersHolder" should "parse parameters" in {
    val args = Array("-study_id", "sample_id1", "sample_id2", "-release_id", "sample_index")

    val holder = new CLIParametersHolder(args)
    holder.study_ids should not be empty
    holder.study_ids.get should contain theSameElementsAs Seq("sample_id1", "sample_id2")
    holder.release_id shouldBe Some("sample_index")
  }


}
