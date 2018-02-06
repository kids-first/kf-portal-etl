package io.kf.etl.common.test.transform

import java.net.URL

import com.trueaccord.scalapb.json.JsonFormat
import io.kf.etl.common.test.common.KfEtlUnitTestSpec
import io.kf.model.Doc
import io.kf.play.Out
import org.json4s.jackson.JsonMethods._

class ScalaPB2Json4STest extends KfEtlUnitTestSpec {

  "A ScalaPB2Json4s.toJsonString() function" should "transform scalapb-compatible entity into json string" in {

    val json = parse(new URL("classpath:///mock_doc_entity.json").openStream())

    import io.kf.etl.common.transform.ScalaPB2Json4s._
    val doc_entity = JsonFormat.fromJson[Doc](json)
    println(doc_entity.toJsonString())

    println(pretty(doc_entity.toJValue()))

  }

}
