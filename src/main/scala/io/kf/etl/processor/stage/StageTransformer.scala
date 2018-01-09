package io.kf.etl.processor.stage

import java.net.URL

import io.kf.etl.processor.Repository
import io.kf.model._
import org.apache.spark.sql.{Dataset, SparkSession}

class StageTransformer(val context:StageContext) {
  def transform(repo: Repository): Dataset[Project] = {
    repo.getPrograms().map(program => {
      repo.getProjectsByProgram(program._2).map(project => {
        val files = repo.getFilesByProject(project._2).toMap

        processSubmittedAlignedReads(context.sparkSession, files.get("submitted_aligned_reads.json").get)

      })
    })
    ???
  }

  private def processSubmittedAlignedReads(spark: SparkSession, file: URL):Dataset[Submitted_Aligned_Reads] = {
    import spark.implicits._
    spark.readStream.option("multiLine", true).json(s"${file.getProtocol}://${file.getPath}").as[String]
    ???
  }

  private def processReadGroup(spark:SparkSession, file:URL): Dataset[Read_Group] = {
    ???
  }

  private def processAliquot(spark:SparkSession, file:URL): Dataset[Aliquot] = {
    ???
  }

  private def processSample(spark:SparkSession, file:URL): Dataset[Sample] = {
    ???
  }

  private def processTrio(spark:SparkSession, file:URL): Dataset[Trio] = {
    ???
  }

  private def processDiagnosis(spark:SparkSession, file:URL): Dataset[Diagnosis] = {
    ???
  }

  private def processDemographic(spark:SparkSession, file:URL): Dataset[Demographic] = {
    ???
  }

  private def processCase(spark:SparkSession, file:URL): Dataset[Case] = {
    ???
  }

}
