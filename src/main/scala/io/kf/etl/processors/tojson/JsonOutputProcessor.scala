package io.kf.etl.processors.tojson

import com.typesafe.config.Config
import io.kf.etl.common.Constants.JSON_OUTPUT_FILES
import io.kf.etl.processors.index.IndexProcessor
import org.apache.spark.sql.{Dataset, SaveMode}


object JsonOutputProcessor {
  def apply[T](
                indexType: String,
                studyId: String,
                releaseId: String
              ) (dataset: Dataset[T])(implicit config: Config): Unit = {
    val partition = IndexProcessor.getIndexName(indexType, studyId, releaseId)

    dataset.write.mode(SaveMode.Overwrite).json(s"${config.getString(JSON_OUTPUT_FILES)}/$partition")
  }

}