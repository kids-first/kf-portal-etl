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

  def transform(data: EntityDataSet):Dataset[ParticipantCentric_ES] = {
    import context.sparkSession.implicits._

    val ctx = StepContext(context.sparkSession, "participantcentric", context.getProcessorDataPath(), context.hdfs, data)

    val (posthandler1, posthandler2) = {
      context.config.write_intermediate_data match {
        case true => ((filename:String) => new WriteKfModelToJsonFile[Participant_ES](ctx, filename), new WriteKfModelToJsonFile[ParticipantCentric_ES](ctx, "final"))
        case false => ((placeholder:String) => new DefaultPostHandler[Dataset[Participant_ES]](), new DefaultPostHandler[Dataset[ParticipantCentric_ES]]())
      }
    }

    Function.chain(
      Seq(
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("01. merge Study into Participant", new MergeStudy(ctx), posthandler1("step1")),
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("02. merge Biospecimen into Participant", new MergeBiospecimenPerParticipant(ctx), posthandler1("step2")),
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("03. merge Diagnosis into Participant", new MergeDiagnosis(ctx), posthandler1("step3")),
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("04. merge Outcome into Participant", new MergeOutcome(ctx), posthandler1("step4")),
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("05. merge Phenotype into Participant", new MergePhenotype(ctx), posthandler1("step5")),
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("06. merge Family into Participant", new MergeFamily(ctx), posthandler1("step6"))
      )
    ).andThen(
      Step[Dataset[Participant_ES], Dataset[ParticipantCentric_ES]]("07. build ParticipantCentric", new BuildParticipantCentric(ctx), posthandler2)
    )(context.sparkSession.emptyDataset[Participant_ES])

  }

}
