package io.kf.etl.processors.participantcommon.transform

import com.typesafe.config.Config
import io.kf.etl.models.es.Participant_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.participantcommon.transform.step._
import org.apache.spark.sql.{Dataset, SparkSession}

object ParticipantCommonTransformer {
  def apply(data: EntityDataSet)(implicit spark: SparkSession, config: Config): Dataset[Participant_ES] = {

    val withStepSink = WriteJsonSink[Participant_ES]("participant_common") _
    val mergedStudy = withStepSink("merge_study")(MergeStudy(data))
    val mergedDiagnosis = withStepSink("merge_diagnosis")(MergeDiagnosis(data, mergedStudy))
    val mergedOutcome = withStepSink("merge_outcome")(MergeOutcome(data, mergedDiagnosis))
    val mergedPhenotypes = withStepSink("merge_phenotypes")(MergePhenotype(data, mergedOutcome))
    val mergedFamily = withStepSink("merge_family")(MergeFamily(data, mergedPhenotypes))
    val mergedBiospecimens = withStepSink("merge_biospecimens")(MergeBiospecimenPerParticipant(data, mergedFamily))

    mergedBiospecimens


  }

}
