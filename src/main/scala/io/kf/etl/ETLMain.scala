package io.kf.etl

import io.kf.etl.context.{CLIParametersHolder, DefaultContext}
import io.kf.etl.processors.download.DownloadProcessor
import io.kf.etl.processors.featurecentric.transform.FeatureCentricTransformer
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.participantcommon.ParticipantCommonProcessor

object ETLMain extends App {

  DefaultContext.withContext { context =>
    import context.implicits._

    import scala.concurrent.ExecutionContext.Implicits._
    lazy val cliArgs: CLIParametersHolder = new CLIParametersHolder(args)

    cliArgs.study_ids match {

      // Requires study_ids to run
      case Some(study_ids) =>
        println(s"Running Pipeline with study IDS {${study_ids.mkString(", ")}}")

        study_ids.foreach { studyId =>
          val dowloadData = DownloadProcessor(studyId)
          val participantCommon = ParticipantCommonProcessor(dowloadData)
          val fileCentric = FeatureCentricTransformer.file(dowloadData, participantCommon)
          val participantCentric = FeatureCentricTransformer.participant(dowloadData, participantCommon)
          IndexProcessor("file_centric", studyId, cliArgs.release_id.get, fileCentric)
          IndexProcessor("participant_centric", studyId, cliArgs.release_id.get, participantCentric)
          spark.sqlContext.clearCache()

        }

      // No Study IDs:
      case None =>
        throw new IllegalArgumentException("No Study IDs provided - Nothing to run.")

    }


  }


}
