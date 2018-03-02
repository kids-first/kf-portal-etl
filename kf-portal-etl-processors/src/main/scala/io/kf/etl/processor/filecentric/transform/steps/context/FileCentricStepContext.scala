package io.kf.etl.processor.filecentric.transform.steps.context

import io.kf.etl.processor.common.ProcessorCommonDefinitions.{DatasetsFromDBTables, ParticipantToGenomicFiles}
import io.kf.etl.processor.filecentric.context.DocumentContext
import org.apache.spark.sql.Dataset

case class FileCentricStepContext(val parentContext: DocumentContext, val dbTables: DatasetsFromDBTables, val participant2GenomicFiles: Dataset[ParticipantToGenomicFiles])
