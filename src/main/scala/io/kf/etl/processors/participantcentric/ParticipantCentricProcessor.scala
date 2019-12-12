package io.kf.etl.processors.participantcentric

import com.typesafe.config.Config
import io.kf.etl.models.es.{ParticipantCentric_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.sink.WriteParquetSink
import io.kf.etl.processors.participantcentric.transform.{ParticipantCentricTransformer, ParticipantCentricTransformerNew}
import org.apache.spark.sql.{Dataset, SparkSession}
object ParticipantCentricProcessor {
  def apply(entityDataSet: EntityDataSet, participants: Dataset[Participant_ES])(implicit config: Config, spark: SparkSession): Dataset[ParticipantCentric_ES] = {
    val transformed = ParticipantCentricTransformerNew(entityDataSet, participants).cache()
    WriteParquetSink("participant_centric", transformed)
    transformed
  }
}
