package io.kf.etl.processors.download.sink

import java.io.File
import java.net.URL

import io.kf.etl.processors.common.ProcessorCommonDefinitions.{DataServiceEntityNames, EntityDataSet}
import io.kf.etl.processors.download.context.DownloadContext
import org.apache.hadoop.fs.Path
import io.kf.etl.common.Constants.HPO_GRAPH_PATH
import io.kf.etl.processors.common.exceptions.KfExceptions.{CreateDumpDirectoryFailedException, DataSinkTargetNotSupportedException}
import org.apache.commons.io.FileUtils

class DownloadSink(val context: DownloadContext) {

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


  def sink(data: EntityDataSet):EntityDataSet = {
    checkSinkDirectory(new URL(context.getJobDataPath()))

    data.participants.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Participant}")
    data.families.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Family}")
    data.biospecimens.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Biospecimen}")
    data.diagnoses.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Diagnosis}")
    data.familyRelationships.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Family_Relationship}")
    data.genomicFiles.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Genomic_File}")
    data.sequencingExperiments.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Sequencing_Experiment}")
    data.studies.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Study}")
    data.studyFiles.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Study_File}")
    data.investigators.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Investigator}")
    data.outcomes.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Outcome}")
    data.phenotypes.write.parquet(s"${context.getJobDataPath()}/${DataServiceEntityNames.Phenotype}")
    data.graphPath.write.parquet(s"${context.getJobDataPath()}/${HPO_GRAPH_PATH}")

    data
  }
}
