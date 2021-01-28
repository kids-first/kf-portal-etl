package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.es.ParticipantCentric_ES
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}

import java.io.File
import scala.reflect.io.Directory

class WriteJsonSinkTest extends FlatSpec with Matchers with WithSparkSession {
import spark.implicits._

  "process" should "merge phenotypes and participant" in {
    val dataset = Seq(
      ParticipantCentric_ES(kf_id = Some("p1"), gender = Some("male")),
      ParticipantCentric_ES(kf_id = Some("p2"), gender = Some("female"), alias_group = Some("ag1")),
      ParticipantCentric_ES(kf_id = Some("p2"), gender = Some("male"), alias_group = Some("ag2"), is_proband = Some(true))
    ).toDS()

    WriteJsonSink.exportDataSetToJsonFile("./src/test/json_output", "file_centric_study1_release1")(dataset)

    val directory = new Directory(new File("./src/test/json_output/file_centric_study1_release1"))
    val outputFileExists = directory.list.map(_.jfile.getName).toList.exists(_.trim.startsWith("part-"))

    directory.deleteRecursively()
    outputFileExists should be(true)
  }

}
