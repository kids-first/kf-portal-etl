package io.kf.etl.processor.document.transform.steps

import io.kf.etl.processor.common.ProcessorCommonDefinitions.{DatasetsFromDBTables, ParticipantToGenomicFiles}
import io.kf.etl.processor.document.context.DocumentContext
import org.apache.spark.sql.Dataset

case class StepContext(val parentContext: DocumentContext, val dbTables: DatasetsFromDBTables, val participant2GenomicFiles: Dataset[ParticipantToGenomicFiles])
