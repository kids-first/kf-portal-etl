package io.kf.etl.processors.download.sink

import com.typesafe.config.Config
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import org.apache.spark.sql.SaveMode

object DownloadSink {

  def apply(data: EntityDataSet, studyId:String)(implicit config: Config): Unit = {

    val sinkPath = config.getString("processors.download.data_path")

    data.participants.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/participants/$studyId")
    data.families.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/families/$studyId")
    data.biospecimens.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/biospecimens/$studyId")
    data.diagnoses.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/diagnoses/$studyId")
    data.biospecimenDiagnoses.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/biospecimens_diagnoses/$studyId")
    data.familyRelationships.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/family_relationships/$studyId")
    data.genomicFiles.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/genomic_files/$studyId")
    data.sequencingExperiments.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/sequencing_experiments/$studyId")
    data.studies.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/studies/$studyId")
    data.studyFiles.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/study_files/$studyId")
    data.investigators.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/investigators/$studyId")
    data.outcomes.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/outcomes/$studyId")
    data.phenotypes.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/phenotypes/$studyId")

  }
}
