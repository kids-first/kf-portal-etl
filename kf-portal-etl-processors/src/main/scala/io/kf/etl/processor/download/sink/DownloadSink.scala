package io.kf.etl.processor.download.sink

import io.kf.etl.processor.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processor.download.context.DownloadContext
import org.apache.hadoop.fs.Path
import io.kf.etl.processor.common.ProcessorCommonDefinitions.DBTables._
import io.kf.etl.common.Constants.HPO_REF_DATA

class DownloadSink(val context: DownloadContext) {
  def sink(data:DatasetsFromDBTables):Unit = {

    context.hdfs.delete(new Path(context.getJobDataPath()), true)

    data.aliquot.write.parquet(s"${context.getJobDataPath()}/${Aliquot.toString}")
    data.demographic.write.parquet(s"${context.getJobDataPath()}/${Demographic.toString}")
    data.diagnosis.write.parquet(s"${context.getJobDataPath()}/${Diagnosis.toString}")
    data.genomicFile.write.parquet(s"${context.getJobDataPath()}/${GenomicFile.toString}")
    data.study.write.parquet(s"${context.getJobDataPath()}/${Study.toString}")
    data.familyRelationship.write.parquet(s"${context.getJobDataPath()}/${FamilyRelationship.toString}")
    data.participant.write.parquet(s"${context.getJobDataPath()}/${Participant.toString}")
//    data.participantAlis.write.parquet(s"${context.getJobDataPath()}/${ParticipantAlias.toString}")
    data.outcome.write.parquet(s"${context.getJobDataPath()}/${Outcome.toString}")
    data.phenotype.write.parquet(s"${context.getJobDataPath()}/${Phenotype.toString}")
    data.sample.write.parquet(s"${context.getJobDataPath()}/${Sample.toString}")
    data.workflow.write.parquet(s"${context.getJobDataPath()}/${Workflow.toString}")
    data.workflowGenomicFile.write.parquet(s"${context.getJobDataPath()}/${WorkflowGenomicFile.toString}")
    data.graphPath.write.parquet(s"${context.getJobDataPath()}/${HPO_REF_DATA}")

  }
}
