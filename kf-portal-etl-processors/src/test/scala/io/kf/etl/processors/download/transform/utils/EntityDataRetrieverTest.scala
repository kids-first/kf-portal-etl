package io.kf.etl.processors.download.transform.utils

import io.kf.etl.common.conf.DataServiceConfig
import io.kf.etl.external.dataservice.entity.EStudy
import io.kf.etl.processors.test.util.{DataService, jsonHandler, jsonHandlerAfterNRetries}
import org.scalatest.{FlatSpec, Matchers}

class EntityDataRetrieverTest extends FlatSpec with Matchers {


  "retrieve" should "return the deserialize data" in {
    DataService.withDataService(Map("/studies" -> jsonHandler(
      """
        |{
        |    "_links": {
        |        "self": "/studies"
        |    },
        |    "_status": {
        |        "code": 200,
        |        "message": "success"
        |    },
        |    "limit": 10,
        |    "results": [
        |        {
        |            "kf_id": "1",
        |            "name": "Study 1"
        |        },
        |        {
        |            "kf_id": "2",
        |            "name": "Study 2"
        |        }
        | ],
        | "total":2
        |}
      """.stripMargin)))(url =>

      EntityDataRetriever(DataServiceConfig(url, 100, "", "")).retrieve[EStudy](Some("/studies")) shouldBe Seq(
        EStudy(kfId = Some("1"), name = Some("Study 1")),
        EStudy(kfId = Some("2"), name = Some("Study 2"))

      )

    )
  }

  it should "return the deserialize data on two pages" in {
    val handlerPage1 = jsonHandler(
      """
        |{
        |    "_links": {
        |        "self": "/studies",
        |        "next": "/studies2"
        |    },
        |    "_status": {
        |        "code": 200,
        |        "message": "success"
        |    },
        |    "limit": 1,
        |    "results": [
        |        {
        |            "kf_id": "1",
        |            "name": "Study 1"
        |        }
        | ],
        | "total":2
        |}
      """.stripMargin)
    val handlerPage2 = jsonHandler(
      """
        |{
        |    "_links": {
        |        "self": "/studies"
        |    },
        |    "_status": {
        |        "code": 200,
        |        "message": "success"
        |    },
        |    "limit": 1,
        |    "results": [
        |        {
        |            "kf_id": "2",
        |            "name": "Study 2"
        |        }
        | ],
        | "total":2
        |}
      """.stripMargin)
    DataService.withDataService(Map(
      "/studies" -> handlerPage1,
      "/studies2" -> handlerPage2
    )) { url =>

      EntityDataRetriever(DataServiceConfig(url, 1, "", "")).retrieve[EStudy](Some("/studies")) shouldBe Seq(
        EStudy(kfId = Some("2"), name = Some("Study 2")),
        EStudy(kfId = Some("1"), name = Some("Study 1"))
      )
      handlerPage1.count shouldBe 1
      handlerPage2.count shouldBe 1

    }
  }

  it should "return the deserialize data after several retries" in {
    val handler = jsonHandlerAfterNRetries(
      """
        |{
        |    "_links": {
        |        "self": "/studies"
        |    },
        |    "_status": {
        |        "code": 200,
        |        "message": "success"
        |    },
        |    "limit": 10,
        |    "results": [
        |        {
        |            "kf_id": "1",
        |            "name": "Study 1"
        |        },
        |        {
        |            "kf_id": "2",
        |            "name": "Study 2"
        |        }
        | ],
        | "total":2
        |}
      """.stripMargin, 2)
    DataService.withDataService(Map("/studies" -> handler)) {
      url =>
        EntityDataRetriever(DataServiceConfig(url, 100, "", "")).retrieve[EStudy](Some("/studies"), retries = 3) shouldBe Seq(
          EStudy(kfId = Some("1"), name = Some("Study 1")),
          EStudy(kfId = Some("2"), name = Some("Study 2"))

        )
        handler.count shouldBe 2


    }
  }
}
