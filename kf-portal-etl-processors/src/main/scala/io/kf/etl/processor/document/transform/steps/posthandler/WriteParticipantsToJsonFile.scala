package io.kf.etl.processor.document.transform.steps.posthandler

import java.io.File
import java.net.URL

import io.kf.etl.model.Participant
import io.kf.etl.processor.document.transform.steps.{StepContext, StepExecutable}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class WriteParticipantsToJsonFile(override val ctx: StepContext, filename:String) extends StepExecutable[Dataset[Participant], Dataset[Participant]]{
  override def process(input: Dataset[Participant]): Dataset[Participant] = {

    import io.kf.etl.transform.ScalaPB2Json4s._
    import ctx.parentContext.sparkSession.implicits._
    val cached = input.cache()
    val target_path = new URL(s"${ctx.parentContext.getJobDataPath()}/steps/${filename}")

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
          ctx.parentContext.hdfs.delete(dir, true)
          ctx.parentContext.hdfs.mkdirs(dir)
          target_path.toString
        }
        case _ => throw StepResultTargetNotSupportedException(target_path)
      }

    cached.map(_.toJsonString()).write.text(json_path)
    cached
  }
}
