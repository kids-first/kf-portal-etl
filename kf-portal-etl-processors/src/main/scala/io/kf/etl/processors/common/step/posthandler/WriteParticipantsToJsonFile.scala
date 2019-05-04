package io.kf.etl.processors.common.step.posthandler

import java.io.File
import java.net.URL

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.context.StepContext
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Dataset

class WriteParticipantsToJsonFile(override val ctx: StepContext, filename: String) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {
  override def process(input: Dataset[Participant_ES]): Dataset[Participant_ES] = {

    import ctx.spark.implicits._
    import io.kf.etl.transform.ScalaPB2Json4s._
    val cached = input.cache()
    val target_path = new URL(s"${ctx.processorDataPath}/steps/$filename")
    val dir = new File(target_path.getFile)
    if (dir.exists())
      FileUtils.deleteDirectory(dir)

    val json_path = dir.getAbsolutePath


    cached.map(_.toJsonString()).write.text(json_path)
    cached
  }
}
