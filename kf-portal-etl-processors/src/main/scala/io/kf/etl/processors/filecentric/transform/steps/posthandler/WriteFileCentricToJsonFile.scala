package io.kf.etl.processors.filecentric.transform.steps.posthandler

import java.io.File
import java.net.URL

import io.kf.etl.model.FileCentric
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.posthandler.StepResultTargetNotSupportedException
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class WriteFileCentricToJsonFile(override val ctx: StepContext) extends StepExecutable[Dataset[FileCentric], Dataset[FileCentric]]{
  override def process(input: Dataset[FileCentric]): Dataset[FileCentric] = {
    import io.kf.etl.transform.ScalaPB2Json4s._
    import ctx.spark.implicits._
    val cached = input.cache()
    val target_path = new URL(s"${ctx.processorDataPath}/steps/${ctx.processorName}")
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
