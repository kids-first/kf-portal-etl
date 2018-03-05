package io.kf.etl.processors.common.step.posthandler

import java.io.File
import java.net.URL

import io.kf.etl.model.Participant
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class WriteParticipantsToJsonFile(override val ctx: StepContext, filename:String) extends StepExecutable[Dataset[Participant], Dataset[Participant]]{
  override def process(input: Dataset[Participant]): Dataset[Participant] = {

    import ctx.spark.implicits._
    import io.kf.etl.transform.ScalaPB2Json4s._
    val cached = input.cache()
    val target_path = new URL(s"${ctx.processorDataPath}/steps/${filename}")

    val json_path =
      target_path.getProtocol match {
        case "file" => {
          val dir = new File(target_path.getFile)
          if(dir.exists())
            FileUtils.deleteDirectory(dir)

          dir.getAbsolutePath
        }
        case "hdfs" => {
          val dir = new Path(target_path.toString)
          ctx.hdfs.delete(dir, true)
          ctx.hdfs.mkdirs(dir)
          target_path.toString
        }
        case _ => throw StepResultTargetNotSupportedException(target_path)
      }

    cached.map(_.toJsonString()).write.text(json_path)
    cached
  }
}
