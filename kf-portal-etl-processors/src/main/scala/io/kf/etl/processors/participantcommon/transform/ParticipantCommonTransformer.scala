package io.kf.etl.processors.participantcommon.transform

import io.kf.etl.es.models.Participant_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.step.Step
import io.kf.etl.processors.common.step.context.StepContext
import io.kf.etl.processors.common.step.impl._
import io.kf.etl.processors.common.step.posthandler.{DefaultPostHandler, WriteKfModelToJsonFile}
import io.kf.etl.processors.participantcommon.context.ParticipantCommonContext
import org.apache.spark.sql.Dataset

class ParticipantCommonTransformer(val context: ParticipantCommonContext) {
  def transform(data: EntityDataSet): (EntityDataSet, Dataset[Participant_ES]) = {

    import context.appContext.sparkSession.implicits._

    val ctx = StepContext(context.appContext.sparkSession, "participantcommon", context.getProcessorDataPath(), data)

    val (posthandler1, posthandler2) = {
      if (context.config.write_intermediate_data) {
        ((filename: String) => new WriteKfModelToJsonFile[Participant_ES](ctx, filename), new WriteKfModelToJsonFile[Participant_ES](ctx, "final"))
      } else {
        ((placeholder: String) => new DefaultPostHandler[Dataset[Participant_ES]](), new DefaultPostHandler[Dataset[Participant_ES]]())
      }
    }

    val ds =
      Function.chain(
        Seq(
          Step[Dataset[Participant_ES], Dataset[Participant_ES]]("01. merge Study into Participant", new MergeStudy(ctx), posthandler1("step1")),
          Step[Dataset[Participant_ES], Dataset[Participant_ES]]("02. merge Diagnosis into Participant", new MergeDiagnosis(ctx), posthandler1("step2")),
          Step[Dataset[Participant_ES], Dataset[Participant_ES]]("03. merge Outcome into Participant", new MergeOutcome(ctx), posthandler1("step3")),
          Step[Dataset[Participant_ES], Dataset[Participant_ES]]("04. merge Phenotype into Participant", new MergePhenotype(ctx), posthandler1("step4")),
          Step[Dataset[Participant_ES], Dataset[Participant_ES]]("05. merge Family into Participant", new MergeFamily(ctx), posthandler1("step5")),
          Step[Dataset[Participant_ES], Dataset[Participant_ES]]("06. merge Biospecimen into Participant", new MergeBiospecimenPerParticipant(ctx), posthandler1("step6"))
        )
      )(context.appContext.sparkSession.emptyDataset[Participant_ES]).cache()

    (data, ds)

  }

}
