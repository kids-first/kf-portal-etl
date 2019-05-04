package io.kf.etl.processors.common.step.posthandler

import java.io.File
import java.net.URL

import com.trueaccord.scalapb.GeneratedMessageCompanion
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.context.StepContext
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Dataset

class WriteKfModelToJsonFile[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]](@transient override val ctx: StepContext, val step_name: String)(implicit meta: GeneratedMessageCompanion[T]) extends StepExecutable[Dataset[T], Dataset[T]] {

  override def process(input: Dataset[T]): Dataset[T] = {
    import ctx.spark.implicits._
    import io.kf.etl.transform.ScalaPB2Json4s._
    val cached = input.cache()
    val target_path = new URL(s"${ctx.processorDataPath}/steps/${ctx.processorName}/$step_name")
    val dir = new File(target_path.getFile)
    if (dir.exists())
      FileUtils.deleteDirectory(dir)

    val json_path = dir.getAbsolutePath
    cached.map(_.toJsonString()).write.text(json_path)
    cached
  }
}
