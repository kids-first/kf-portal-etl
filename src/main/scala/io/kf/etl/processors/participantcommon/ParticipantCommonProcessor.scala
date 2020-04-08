package io.kf.etl.processors.participantcommon

import com.typesafe.config.Config
import io.kf.etl.models.es.Participant_ES
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.participantcommon.transform.ParticipantCommonTransformer
import org.apache.spark.sql.{Dataset, SparkSession}


object ParticipantCommonProcessor {

  def apply(entityDataSet: EntityDataSet)(implicit config: Config, spark: SparkSession): Dataset[Participant_ES] = {
    val transformed = ParticipantCommonTransformer(entityDataSet).cache()
//    WriteParquetSink("participant_common", transformed)
    transformed
  }
}
