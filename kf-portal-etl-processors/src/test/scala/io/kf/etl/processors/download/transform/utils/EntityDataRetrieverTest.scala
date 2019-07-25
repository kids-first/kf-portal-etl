package io.kf.etl.processors.download.transform.utils

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.kf.etl.common.conf.DataServiceConfig
import io.kf.etl.external.dataservice.entity.{EBiospecimenDiagnosis, EStudy}
import io.kf.etl.processors.test.util.{DataService, jsonHandler, jsonHandlerAfterNRetries}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.libs.ws.StandaloneWSClient
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.DurationInt

class EntityDataRetrieverTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val wsClient: StandaloneWSClient = StandaloneAhcWSClient()

  override def afterAll(): Unit = {
    wsClient.close()
    system.terminate()
  }


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
      """.stripMargin))) { url =>


      val result = Await.result(
        EntityDataRetriever(DataServiceConfig(url, 100, "", "")).retrieve[EStudy]("/studies"), 60.seconds)

      result shouldBe Seq(
        EStudy(kfId = Some("1"), name = Some("Study 1")),
        EStudy(kfId = Some("2"), name = Some("Study 2"))

      )

    }
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

      EntityDataRetriever(DataServiceConfig(url, 1, "", "")).retrieve[EStudy]("/studies").map {
        r =>
          r shouldBe Seq(
            EStudy(kfId = Some("1"), name = Some("Study 1")),
            EStudy(kfId = Some("2"), name = Some("Study 2"))
          )
          handlerPage1.count shouldBe 1
          handlerPage2.count shouldBe 1
      }
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
        EntityDataRetriever(DataServiceConfig(url, 100, "", "")).retrieve[EStudy]("/studies", retries = 3).map {
          r =>
            r shouldBe Seq(
              EStudy(kfId = Some("1"), name = Some("Study 1")),
              EStudy(kfId = Some("2"), name = Some("Study 2"))

            )
            handler.count shouldBe 2
        }


    }
  }

  it should "return the deserialize data for biospecimen-diagnoses" in {
    val handler = jsonHandler(
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
        |            "_links": {
        |                "biospecimen": "/biospecimens/BS_CGXTFM67",
        |                "collection": "/biospecimen-diagnoses",
        |                "diagnosis": "/diagnoses/DG_BXRD7128",
        |                "self": "/biospecimen-diagnoses/BD_EY46KMKQ"
        |            },
        |            "created_at": "2018-11-09T18:06:34.985858+00:00",
        |            "external_id": "BS_CGXTFM67:DG_BXRD7128",
        |            "kf_id": "BD_EY46KMKQ",
        |            "modified_at": "2019-07-18T20:48:24.668603+00:00",
        |            "visible": true
        |        },
        |        {
        |            "_links": {
        |                "biospecimen": "/biospecimens/BS_3Z40EZHD",
        |                "collection": "/biospecimen-diagnoses",
        |                "diagnosis": "/diagnoses/DG_PBK7GH8K",
        |                "self": "/biospecimen-diagnoses/BD_HTXNZM74"
        |            },
        |            "created_at": "2018-11-09T18:06:35.152377+00:00",
        |            "external_id": "BS_3Z40EZHD:DG_PBK7GH8K",
        |            "kf_id": "BD_HTXNZM74",
        |            "modified_at": "2019-07-18T20:48:25.111116+00:00",
        |            "visible": true
        |        }
        | ],
        | "total":2
        |}
      """.stripMargin)
    DataService.withDataService(Map("/biospecimen-diagnoses" -> handler)) {
      url =>

        val result = Await.result(
          EntityDataRetriever(DataServiceConfig(url, 100, "", "")).retrieve[EBiospecimenDiagnosis]("/biospecimen-diagnoses"), 60.seconds)
        result shouldBe Seq(
          EBiospecimenDiagnosis(kfId = Some("BD_EY46KMKQ"), biospecimenId = Some("BS_CGXTFM67"), diagnosisId = Some("DG_BXRD7128"), visible = Some(true)),
          EBiospecimenDiagnosis(kfId = Some("BD_HTXNZM74"), biospecimenId = Some("BS_3Z40EZHD"), diagnosisId = Some("DG_PBK7GH8K"), visible = Some(true) )

        )


    }
  }
}
