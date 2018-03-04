package io.kf.etl.processors.download.sink

import java.io.File
import java.net.URL

import io.kf.etl.processors.common.ProcessorCommonDefinitions.DatasetsFromDBTables
import io.kf.etl.processors.download.context.DownloadContext
import org.apache.hadoop.fs.Path
import io.kf.etl.processors.common.ProcessorCommonDefinitions.PostgresqlDBTables._
import io.kf.etl.common.Constants.HPO_GRAPH_PATH
import io.kf.etl.processors.common.exceptions.KfExceptions.{CreateDumpDirectoryFailedException, DataSinkTargetNotSupportedException}
import org.apache.commons.io.FileUtils

class DownloadSink(val context: DownloadContext) {
  def sink(data:DatasetsFromDBTables):Unit = {

    checkSinkDirectory(new URL(context.getJobDataPath()))

    data.aliquot.write.parquet(s"${context.getJobDataPath()}/${Aliquot.toString}")
    data.demographic.write.parquet(s"${context.getJobDataPath()}/${Demographic.toString}")
    data.diagnosis.write.parquet(s"${context.getJobDataPath()}/${Diagnosis.toString}")
    data.genomicFile.write.parquet(s"${context.getJobDataPath()}/${Genomic_File.toString}")
    data.study.write.parquet(s"${context.getJobDataPath()}/${Study.toString}")
    data.familyRelationship.write.parquet(s"${context.getJobDataPath()}/${Family_Relationship.toString}")
    data.participant.write.parquet(s"${context.getJobDataPath()}/${Participant.toString}")
//    data.participantAlis.write.parquet(s"${context.getJobDataPath()}/${Participant_Alias.toString}")
    data.outcome.write.parquet(s"${context.getJobDataPath()}/${Outcome.toString}")
    data.phenotype.write.parquet(s"${context.getJobDataPath()}/${Phenotype.toString}")
    data.sample.write.parquet(s"${context.getJobDataPath()}/${Sample.toString}")
    data.workflow.write.parquet(s"${context.getJobDataPath()}/${Workflow.toString}")
    data.workflowGenomicFile.write.parquet(s"${context.getJobDataPath()}/${Workflow_Genomic_File.toString}")
    data.sequencingExperiment.write.parquet(s"${context.getJobDataPath()}/${Sequencing_Experiment.toString}")
    data.graphPath.write.parquet(s"${context.getJobDataPath()}/${HPO_GRAPH_PATH}")

  }

  private def checkSinkDirectory(url: URL):Unit = {
    val url = new URL(context.getJobDataPath())

    url.getProtocol match {
      case "hdfs" => {
        val dir = new Path(url.toString)
        context.hdfs.delete(dir, true)
        context.hdfs.mkdirs(dir)
      }
      case "file" => {
        val dir = new File(url.getFile)
        if(dir.exists())
          FileUtils.deleteDirectory(dir)
        dir.mkdir() match {
          case false => throw CreateDumpDirectoryFailedException(url)
          case true =>
        }
      }
      case value => DataSinkTargetNotSupportedException(url)
    }
  }
}
