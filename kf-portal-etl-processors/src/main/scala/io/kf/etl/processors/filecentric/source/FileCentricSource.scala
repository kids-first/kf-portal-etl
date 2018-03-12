package io.kf.etl.processors.filecentric.source

import java.util.Formatter

import io.kf.etl.common.Constants.HPO_GRAPH_PATH
import io.kf.etl.processors.common.ProcessorCommonDefinitions._
import io.kf.etl.processors.filecentric.context.FileCentricContext
import io.kf.etl.processors.repo.Repository
import io.kf.etl.dbschema._
import io.kf.etl.model.utils.{GenomicFileToStudy, GenomicFileToStudyId, ParticipantToGenomicFiles, TransformedGraphPath}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.PostgresqlDBTables._
import org.apache.spark.sql.Dataset


class FileCentricSource(val context: FileCentricContext) {

  def source(repo:Repository): DatasetsFromDBTables = {
    import context.sparkSession.implicits._

    val input_path =
      repo.url.getProtocol match {
        case "file" => repo.url.getFile
        case _ => repo.url.toString
      }

      val study = context.sparkSession.read.parquet(s"${input_path}/${Study.toString}").as[TStudy].cache()
      val participant = context.sparkSession.read.parquet(s"${input_path}/${Participant.toString}").as[TParticipant].cache()
      val demographic = context.sparkSession.read.parquet(s"${input_path}/${Demographic.toString}").as[TDemographic].cache()
      val sample = context.sparkSession.read.parquet(s"${input_path}/${Sample.toString}").as[TSample].cache()
      val aliquot = context.sparkSession.read.parquet(s"${input_path}/${Aliquot.toString}").as[TAliquot].cache()
      val sequencing_experiment = context.sparkSession.read.parquet(s"${input_path}/${Sequencing_Experiment.toString}").as[TSequencingExperiment].cache()
      val diagnosis = context.sparkSession.read.parquet(s"${input_path}/${Diagnosis.toString}").as[TDiagnosis].cache()
      val phenotype = context.sparkSession.read.parquet(s"${input_path}/${Phenotype.toString}").as[TPhenotype].cache()
      val genomic_file = context.sparkSession.read.parquet(s"${input_path}/${Genomic_File.toString}").as[TGenomicFile].cache()
      val family_relationship = context.sparkSession.read.parquet(s"${input_path}/${Family_Relationship.toString}").as[TFamilyRelationship].cache()
      val hpo_graph_path = context.sparkSession.read.parquet(s"${input_path}/${HPO_GRAPH_PATH}").as[TransformedGraphPath].cache()
//      val participant_alias = context.sparkSession.read.parquet(s"${input_path}/${Participant_Alias.toString}").as[TParticipantAlias].cache()

    study.createOrReplaceTempView("ST")
    participant.createOrReplaceTempView("PAR")
    demographic.createOrReplaceTempView("DG")
    sample.createOrReplaceTempView("SA")
    aliquot.createOrReplaceTempView("AL")
    sequencing_experiment.createOrReplaceTempView("SE")
    diagnosis.createOrReplaceTempView("DI")
    phenotype.createOrReplaceTempView("PT")
    //    outcome.createOrReplaceTempView("OC")
    genomic_file.createOrReplaceTempView("GF")
    //    workflow.createOrReplaceTempView("WF")
    family_relationship.createOrReplaceTempView("FR")
    //    workflowGenomicFile.createOrReplaceTempView("WG")

    val (genomicfile_study ,participant_genomicfile) = mapParticipantAndGenomicFile(study)
    
    DatasetsFromDBTables(
      study,
      participant,
      demographic,
      sample,
      aliquot,
      sequencing_experiment,
      diagnosis,
      phenotype,
      null,
      genomic_file,
      null,
      family_relationship,
      null,
//      participant_alias,
      hpo_graph_path,
      participant_genomicfile,
      genomicfile_study
    )

  }

  private def mapParticipantAndGenomicFile(study: Dataset[TStudy]): (Dataset[GenomicFileToStudy], Dataset[ParticipantToGenomicFiles]) = {
    val sql =
      """
         select PAR.kfId, PAR.studyId, AA.gfId, AA.dataType from PAR left join
           (select SA.participantId as kfId, BB.gfId, BB.dataType from SA left join
             (select AL.sampleId as kfId, CC.gfId, CC.dataType from AL left join
                (select SE.aliquotId as kfId, GF.kfId as gfId, GF.dataType from SE left join GF on SE.kfId = GF.sequencingExperimentId) as CC
               on AL.kfId = CC.kfId) as BB
             on SA.kfId = BB.kfId ) as AA
          on PAR.kfId = AA.kfId
      """.stripMargin

    import context.sparkSession.implicits._

    val raw = context.sparkSession.sql(sql).cache();

    val gf2StudyId =
      raw.groupByKey(_.getString(2)).mapGroups((fileId, iterator) => {
        val list = iterator.toList.collect{
          case row if(row.getString(1) != null) => row.getString(1)
        }.toSet.toSeq
        GenomicFileToStudyId(
          fileId,
          list.size match {
            case 0 => None
            case _ => Some(list(0))
          }
        )
      })

    val gf2Study =
      gf2StudyId.joinWith(study, gf2StudyId.col("studyId") === study.col("kfId")).map(tuple => {
        val tStudy = tuple._2
        GenomicFileToStudy(
          tuple._1.kfId,
          io.kf.etl.model.Study(
            kfId = tStudy.kfId,
            uuid = tStudy.uuid,
            createdAt = tStudy.createdAt,
            modifiedAt = tStudy.modifiedAt,
            name = tStudy.name,
            version = tStudy.version,
            attribution = tStudy.attribution,
            externalId = tStudy.externalId,
            dataAccessAuthority = tStudy.dataAccessAuthority
          )
        )
    }).cache()

    Tuple2(
      gf2Study,

    raw.groupByKey(_.getString(0)).mapGroups((par_id, iterator) => {

      val list = iterator.toList

      ParticipantToGenomicFiles(
        par_id,
        list.collect{
          case row if(row.getString(2) != null) => row.getString(2)
        }.toSet.toSeq,
        list.collect{
          case row if(row.getString(3) != null) => row.getString(3)
        }.toSet.toSeq
      )
    }).cache()
    )
  }

}
