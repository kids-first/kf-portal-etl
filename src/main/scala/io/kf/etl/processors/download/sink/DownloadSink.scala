package io.kf.etl.processors.download.sink

import com.typesafe.config.Config
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import org.apache.spark.sql.SaveMode

object DownloadSink {

  def apply(data: EntityDataSet)(implicit config: Config): Unit = {

    val sinkPath = config.getString("processors.download.data_path")

    data.participants.write.mode(SaveMode.Overwrite).parquet(s"$sinkPath/participant")
    data.families.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/family")
    data.biospecimens.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/biospecimen")
    data.diagnoses.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/diagnosis")
    data.familyRelationships.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"family_relationship")
    data.genomicFiles.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/genomic_file")
    data.sequencingExperiments.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/sequencing_experiment")
    data.studies.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/study")
    data.studyFiles.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/study_file")
    data.investigators.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/investigator")
    data.outcomes.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/outcome")
    data.phenotypes.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/phenotype")
    data.ontologyData.hpoTerms.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/hpo")
    data.ontologyData.mondoTerms.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/mondo")
    data.ontologyData.ncitTerms.write.mode(SaveMode.Overwrite).mode(SaveMode.Overwrite).parquet(s"$sinkPath/ncit")


  }
}
