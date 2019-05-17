package io.kf.etl.processors.participantcentric.transform

import io.kf.etl.es.models.{ParticipantCentric_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.step.Step
import io.kf.etl.processors.common.step.context.StepContext
import io.kf.etl.processors.common.step.impl._
import io.kf.etl.processors.common.step.posthandler.{DefaultPostHandler, WriteKfModelToJsonFile}
import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext
import io.kf.etl.processors.participantcentric.transform.steps.BuildParticipantCentric
import org.apache.spark.sql.Dataset

class ParticipantCentricTransformer(val context: ParticipantCentricContext) {

  def transform(data: (EntityDataSet, Dataset[Participant_ES])): Dataset[ParticipantCentric_ES] = {

    val ctx = StepContext(context.appContext.sparkSession, "participantcentric", context.getProcessorDataPath(), data._1)

    val posthandler = {
      if (context.config.write_intermediate_data) {
        new WriteKfModelToJsonFile[ParticipantCentric_ES](ctx, "final")
      } else {
        new DefaultPostHandler[Dataset[ParticipantCentric_ES]]()
      }
    }

    Step[Dataset[Participant_ES], Dataset[ParticipantCentric_ES]]("02. build ParticipantCentric", new BuildParticipantCentric(ctx), posthandler)(data._2)

  }

}
