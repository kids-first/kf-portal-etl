package io.kf.etl.processors.filecentric.transform

import io.kf.etl.es.models.{FileCentric_ES, Participant_ES}
import io.kf.etl.model.utils.SeqExpId_FileCentricES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.step.Step
import io.kf.etl.processors.common.step.impl._
import io.kf.etl.processors.common.step.posthandler.{DefaultPostHandler, WriteKfModelToJsonFile}
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import io.kf.etl.processors.filecentric.context.FileCentricContext
import io.kf.etl.processors.filecentric.transform.steps._
import org.apache.spark.sql.Dataset

class FileCentricTransformer(val context: FileCentricContext) {
  def transform(data: EntityDataSet):Dataset[FileCentric_ES] = {

    import context.sparkSession.implicits._

    val ctx = StepContext(context.sparkSession, "filecentric", context.getProcessorDataPath(), context.hdfs, data)

    val (posthandler1, posthandler2, posthandler3) = {
      context.config.write_intermediate_data match {
        case true => ((filename:String) => new WriteKfModelToJsonFile[Participant_ES](ctx), new WriteKfModelToJsonFile[SeqExpId_FileCentricES](ctx), new WriteKfModelToJsonFile[FileCentric_ES](ctx))
        case false => ((placeholder:String) => new DefaultPostHandler[Dataset[Participant_ES]](), new DefaultPostHandler[Dataset[SeqExpId_FileCentricES]](), new DefaultPostHandler[Dataset[FileCentric_ES]]())
      }
    }

    Function.chain(
      Seq(
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("01. merge Study into Participant", new MergeStudy(ctx), posthandler1("step1")),
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("02. merge Diagnosis into Participant", new MergeDiagnosis(ctx), posthandler1("step2")),
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("03. merge Outcome into Participant", new MergeOutcome(ctx), posthandler1("step3")),
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("04. merge Phenotype into Participant", new MergePhenotype(ctx), posthandler1("step4")),
        Step[Dataset[Participant_ES], Dataset[Participant_ES]]("05. merge Family into Participant", new MergeFamily(ctx), posthandler1("step5"))
      )
    ).andThen(
      Step[Dataset[Participant_ES], Dataset[SeqExpId_FileCentricES]]("06. build FileCentric ", new BuildFileCentric(ctx), posthandler2)
    ).andThen(
      Step[Dataset[SeqExpId_FileCentricES], Dataset[FileCentric_ES]]("07. merge SequencingExperiment into FileCentric ", new MergeSeqExp(ctx), posthandler3)
    )(context.sparkSession.emptyDataset[Participant_ES])

  }
}
