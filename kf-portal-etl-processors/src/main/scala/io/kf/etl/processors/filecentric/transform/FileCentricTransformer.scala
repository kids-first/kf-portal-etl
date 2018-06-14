package io.kf.etl.processors.filecentric.transform

import io.kf.etl.es.models.{FileCentric_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.step.Step
import io.kf.etl.processors.common.step.impl._
import io.kf.etl.processors.common.step.posthandler.{DefaultPostHandler, WriteKfModelToJsonFile}
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import io.kf.etl.processors.filecentric.context.FileCentricContext
import io.kf.etl.processors.filecentric.transform.steps._
import org.apache.spark.sql.Dataset

class FileCentricTransformer(val context: FileCentricContext) {
  def transform(data: (EntityDataSet, Dataset[Participant_ES])):Dataset[FileCentric_ES] = {

    val ctx = StepContext(context.appContext.sparkSession, "filecentric", context.getProcessorDataPath(), context.appContext.hdfs, data._1)

    val (posthandler1, posthandler2) = {
      context.config.write_intermediate_data match {
        case true => ((filename:String) => new WriteKfModelToJsonFile[Participant_ES](ctx, filename), new WriteKfModelToJsonFile[FileCentric_ES](ctx, "final"))
        case false => ((placeholder:String) => new DefaultPostHandler[Dataset[Participant_ES]](), new DefaultPostHandler[Dataset[FileCentric_ES]]())
      }
    }

    Step[Dataset[Participant_ES], Dataset[FileCentric_ES]]("01. build FileCentric ", new BuildFileCentric(ctx), posthandler2)(data._2)

  }
}
