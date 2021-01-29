package io.kf.etl

import io.kf.etl.common.Constants.SAVE_JSON_FILES
import io.kf.etl.context.{CLIParametersHolder, DefaultContext}
import io.kf.etl.processors.download.DownloadProcessor
import io.kf.etl.processors.featurecentric.FeatureCentricProcessor
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.participantcommon.ParticipantCommonProcessor
import io.kf.etl.processors.tojson.JsonOutputProcessor

import scala.util.Try

object ETLMain extends App {

  DefaultContext.withContext { context =>
    import context.implicits._

    import scala.concurrent.ExecutionContext.Implicits._

    val saveJsonToS3 = Try(config.getBoolean(SAVE_JSON_FILES)).getOrElse(false)

    lazy val cliArgs: CLIParametersHolder = new CLIParametersHolder(args)

    cliArgs.study_ids match {

      // Requires study_ids to run
      case Some(study_ids) =>
        println(s"Running Pipeline with study IDS {${study_ids.mkString(", ")}}")

        study_ids.foreach { studyId =>
          val downloadData = DownloadProcessor(studyId)
          val participantCommon = ParticipantCommonProcessor(downloadData)
          val fileCentric = FeatureCentricProcessor.fileCentric(downloadData, participantCommon)
          val participantCentric = FeatureCentricProcessor.participantCentric(downloadData, participantCommon)
          val studyCentric = FeatureCentricProcessor.studyCentric(
            downloadData,
            studyId,
            participantCentric,
            fileCentric
          )
          IndexProcessor("file_centric", studyId, cliArgs.release_id.get, fileCentric)
          IndexProcessor("participant_centric", studyId, cliArgs.release_id.get, participantCentric)
          IndexProcessor("study_centric", studyId, cliArgs.release_id.get, studyCentric)



          if (saveJsonToS3) {
            val datasets = Map (
              "file_centric" -> fileCentric,
              "participant_centric" -> participantCentric,
              "study_centric" -> studyCentric
            )
            datasets.foreach(d => JsonOutputProcessor(d._1, studyId, cliArgs.release_id.get)(d._2))
          }

          spark.sqlContext.clearCache()
        }

      // No Study IDs:
      case None =>
        throw new IllegalArgumentException("No Study IDs provided - Nothing to run.")

    }


  }


}
