package io.kf.etl.common.test.transform

import com.trueaccord.scalapb.json.JsonFormat
import io.kf.etl.common.test.common.KfEtlUnitTestSpec
import io.kf.model.Doc

class ScalaPB2Json4STest extends KfEtlUnitTestSpec{

  import io.kf.etl.common.transform.ScalaPB2Json4s._
  JsonFormat.fromJsonString[Doc]("").toJsonString()
}
