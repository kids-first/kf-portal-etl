package io.kf.etl.processors.participantcentric.transform

import io.kf.etl.model.{FileCentric, Participant, ParticipantCentric}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processors.common.step.Step
import io.kf.etl.processors.common.step.impl._
import io.kf.etl.processors.common.step.posthandler.{DefaultPostHandler, WriteKfModelToJsonFile}
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import io.kf.etl.processors.participantcentric.context.ParticipantCentricContext
import io.kf.etl.processors.participantcentric.transform.steps.BuildParticipantCentric
import org.apache.spark.sql.Dataset

class ParticipantCentricTransformer(val context: ParticipantCentricContext) {
  def transform(input: DatasetsFromDBTables): Dataset[ParticipantCentric] = {
    import context.sparkSession.implicits._

    val ctx = StepContext(context.sparkSession, "participantcentric", context.getProcessorDataPath(), context.hdfs, input)

    //    val (posthandler1, posthandler2) = {
    //      context.config.write_intermediate_data match {
    //        case true => ((filename:String) => new WriteParticipantsToJsonFile(ctx, filename), new WriteFileCentricToJsonFile(ctx))
    //        case false => ((placeholder:String) => new DefaultPostHandler[Dataset[Participant]](), new DefaultPostHandler[Dataset[FileCentric]]())
    //      }
    //    }
    val (posthandler1, posthandler2) = {
      context.config.write_intermediate_data match {
        case true => ((filename:String) => new WriteKfModelToJsonFile[Participant](ctx), new WriteKfModelToJsonFile[ParticipantCentric](ctx))
        case false => ((placeholder:String) => new DefaultPostHandler[Dataset[Participant]](), new DefaultPostHandler[Dataset[ParticipantCentric]]())
      }
    }

    Function.chain(
      Seq(
        Step[Dataset[Participant], Dataset[Participant]]("01. merge Study into Participant", new MergeStudy(ctx), posthandler1("step1")),
        Step[Dataset[Participant], Dataset[Participant]]("02. merge Demographic into Participant", new MergeDemographic(ctx), posthandler1("step2")),
        Step[Dataset[Participant], Dataset[Participant]]("03. merge Diagnosis into Participant", new MergeDiagnosis(ctx), posthandler1("step3")),
        Step[Dataset[Participant], Dataset[Participant]]("04. compute HPO reference data and then merge Phenotype into Participant", new MergePhenotype(ctx), posthandler1("step4")),
        Step[Dataset[Participant], Dataset[Participant]]("05. merge 'availableDataTypes' into Participant", new MergeAvailableDataTypesForParticipant(ctx), posthandler1("step5")),
        Step[Dataset[Participant], Dataset[Participant]]("06. merge family member into Participant", new MergeFamilyMember(ctx), posthandler1("step6")),
        Step[Dataset[Participant], Dataset[Participant]]("07. merge Sample, Aliquot into Participant", new MergeSample(ctx), posthandler1("step7"))
      )
    ).andThen(
      Step[Dataset[Participant], Dataset[ParticipantCentric]]("08. build final ParticipantCentric", new BuildParticipantCentric(ctx), posthandler2)
    )(context.sparkSession.emptyDataset[Participant])
  }

}
