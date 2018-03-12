package io.kf.etl.processors.filecentric.transform

import io.kf.etl.model.utils.GfId_Participants
import io.kf.etl.model.{FileCentric, Participant}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processors.common.step.Step
import io.kf.etl.processors.common.step.impl._
import io.kf.etl.processors.common.step.posthandler.{DefaultPostHandler, WriteKfModelToJsonFile, WriteParticipantsToJsonFile}
import io.kf.etl.processors.filecentric.context.FileCentricContext
import io.kf.etl.processors.filecentric.transform.steps.{BuildFileCentric, BuildFileCentricNew, MergeSampleAliquot}
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class FileCentricTransformer(val context: FileCentricContext) {

  def transform(input: DatasetsFromDBTables): Dataset[FileCentric] = {

    import context.sparkSession.implicits._

    val ctx = StepContext(context.sparkSession, "filecentric", context.getProcessorDataPath(), context.hdfs, input)

    val (posthandler1, posthandler2) = {
      context.config.write_intermediate_data match {
        case true => ((filename:String) => new WriteKfModelToJsonFile[Participant](ctx), new WriteKfModelToJsonFile[FileCentric](ctx))
        case false => ((placeholder:String) => new DefaultPostHandler[Dataset[Participant]](), new DefaultPostHandler[Dataset[FileCentric]]())
      }
    }

//    Function.chain(
//      Seq(
//        Step[Dataset[Participant], Dataset[Participant]]("01. merge Study into Participant", new MergeStudy(ctx), posthandler1("step1")),
//        Step[Dataset[Participant], Dataset[Participant]]("02. merge Demographic into Participant", new MergeDemographic(ctx), posthandler1("step2")),
//        Step[Dataset[Participant], Dataset[Participant]]("03. merge Diagnosis into Participant", new MergeDiagnosis(ctx), posthandler1("step3")),
//        Step[Dataset[Participant], Dataset[Participant]]("04. compute HPO reference data and then merge Phenotype into Participant", new MergePhenotype(ctx), posthandler1("step4")),
//        Step[Dataset[Participant], Dataset[Participant]]("05. merge 'availableDataTypes' into Participant", new MergeAvailableDataTypesForParticipant(ctx), posthandler1("step5")),
//        Step[Dataset[Participant], Dataset[Participant]]("06. merge family member into Participant", new MergeFamilyMember(ctx), posthandler1("step6")),
//        Step[Dataset[Participant], Dataset[Participant]]("07. merge Sample, Aliquot into Participant", new MergeSample(ctx), posthandler1("step7"))
//      )
//    ).andThen(
//      Step[Dataset[Participant], Dataset[FileCentric]]("08. build final FileCentric", new BuildFileCentric(ctx), posthandler2)
//    )(context.sparkSession.emptyDataset[Participant])


    Function.chain(
      Seq(
        Step[Dataset[Participant], Dataset[Participant]]("01. merge Study into Participant", new MergeStudy(ctx), posthandler1("step1")),
        Step[Dataset[Participant], Dataset[Participant]]("02. merge Demographic into Participant", new MergeDemographic(ctx), posthandler1("step2")),
        Step[Dataset[Participant], Dataset[Participant]]("03. merge Diagnosis into Participant", new MergeDiagnosis(ctx), posthandler1("step3")),
        Step[Dataset[Participant], Dataset[Participant]]("04. compute HPO reference data and then merge Phenotype into Participant", new MergePhenotype(ctx), posthandler1("step4")),
        Step[Dataset[Participant], Dataset[Participant]]("05. merge 'availableDataTypes' into Participant", new MergeAvailableDataTypesForParticipant(ctx), posthandler1("step5")),
        Step[Dataset[Participant], Dataset[Participant]]("06. merge family member into Participant", new MergeFamilyMember(ctx), posthandler1("step6"))
      )
    ).andThen(
      {
        val ph = (filename:String) => new WriteKfModelToJsonFile[GfId_Participants](ctx)
        Step[Dataset[Participant], Dataset[GfId_Participants]]("07. merge Sample, Aliquot into Participant", new MergeSampleAliquot(ctx), ph("step7"))
      }
    ).andThen(
      Step[Dataset[GfId_Participants], Dataset[FileCentric]]("08. build final FileCentric", new BuildFileCentricNew(ctx), posthandler2)
    )(context.sparkSession.emptyDataset[Participant])

  }

}
