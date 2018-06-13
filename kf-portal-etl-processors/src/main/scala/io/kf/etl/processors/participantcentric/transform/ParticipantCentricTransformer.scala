package io.kf.etl.processors.participantcentric.transform

import io.kf.etl.es.models.{ParticipantCentric_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.step.Step
import io.kf.etl.processors.common.step.impl._
import io.kf.etl.processors.common.step.posthandler.{DefaultPostHandler, WriteKfModelToJsonFile}
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext
import io.kf.etl.processors.participantcentric.transform.steps.BuildParticipantCentric
import org.apache.spark.sql.Dataset

class ParticipantCentricTransformer(val context: ParticipantCentricContext) {

  def transform(data: (EntityDataSet, Dataset[Participant_ES])):Dataset[ParticipantCentric_ES] = {

    val ctx = StepContext(context.appContext.sparkSession, "participantcentric", context.getProcessorDataPath(), context.appContext.hdfs, data._1)

    val (posthandler1, posthandler2) = {
      context.config.write_intermediate_data match {
        case true => ((filename:String) => new WriteKfModelToJsonFile[Participant_ES](ctx, filename), new WriteKfModelToJsonFile[ParticipantCentric_ES](ctx, "final"))
        case false => ((placeholder:String) => new DefaultPostHandler[Dataset[Participant_ES]](), new DefaultPostHandler[Dataset[ParticipantCentric_ES]]())
      }
    }

    Function.chain(
      Seq(
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("01. merge Biospecimen into Participant", new MergeBiospecimenPerParticipant(ctx), posthandler1("step2"))
      )
    ).andThen(
      Step[Dataset[Participant_ES], Dataset[ParticipantCentric_ES]]("02. build ParticipantCentric", new BuildParticipantCentric(ctx), posthandler2)
    )(data._2)

  }

}
