package io.kf.etl.processor.filecentric.transform.steps.posthandler

import java.io.File
import java.net.URL

import io.kf.etl.model.filecentric.FileCentric
import io.kf.etl.processor.filecentric.transform.steps.StepExecutable
import io.kf.etl.processor.filecentric.transform.steps.context.FileCentricStepContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class WriteFileCentricToJsonFile(override val ctx: FileCentricStepContext) extends StepExecutable[Dataset[FileCentric], Dataset[FileCentric]]{
  override def process(input: Dataset[FileCentric]): Dataset[FileCentric] = {
    import io.kf.etl.transform.ScalaPB2Json4s._
    import ctx.parentContext.sparkSession.implicits._
    val cached = input.cache()
    val target_path = new URL(s"${ctx.parentContext.getProcessorDataPath()}/steps/filecentric")
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
