package io.kf.etl.processors.filecentric_new.transform.steps

import io.kf.etl.es.models.{FileCentric_ES, Participant_ES}
import io.kf.etl.model.utils.SeqExpId_FileCentricES
import io.kf.etl.processors.common.converter.PBEntityConverter
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.sql.Dataset

class MergeSeqExp(override val ctx: StepContext) extends StepExecutable[Dataset[SeqExpId_FileCentricES], Dataset[FileCentric_ES]] {
  override def process(input: Dataset[SeqExpId_FileCentricES]): Dataset[FileCentric_ES] = {

    import ctx.spark.implicits._
    input.joinWith(
      ctx.entityDataset.sequencingExperiments,
      input.col("seqExpId") === ctx.entityDataset.sequencingExperiments.col("kfId"),
      "left"
    ).groupByKey(_._1.filecentric.kfId).mapGroups((_, iterator) => {
      val seq = iterator.toSeq

      val filecentric = seq(0)._1.filecentric

      filecentric.copy(
        sequencingExperiments = seq.map(tuple => {
          PBEntityConverter.ESequencingExperimentToSequencingExperimentES( tuple._2 )
        })
      )

    })

  }
}
