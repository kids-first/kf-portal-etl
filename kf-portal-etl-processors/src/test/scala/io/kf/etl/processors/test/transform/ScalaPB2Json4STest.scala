package io.kf.etl.processors.test.transform

import java.net.URL

import io.kf.etl.test.common.KfEtlUnitTestSpec
import io.kf.test.Doc
import org.json4s.jackson.JsonMethods._

class ScalaPB2Json4STest extends KfEtlUnitTestSpec {

  "A ScalaPB2Json4s.toJsonString() function" should "transform scalapb-compatible entity into json string" in {

    val json = parse(new URL("classpath:///mock_doc_entity.json").openStream())

    import io.kf.etl.transform.ScalaPB2Json4s._
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
    println(testDoc.toJsonString())

    println(pretty(testDoc.toJValue()))

  }

}
