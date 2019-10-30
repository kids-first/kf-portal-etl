package io.kf.etl

import io.kf.etl.context.{CLIParametersHolder, DefaultContext}
import io.kf.etl.processors.download.DownloadProcessor
import io.kf.etl.processors.filecentric.FileCentricProcessor
import io.kf.etl.processors.index.IndexProcessor
import io.kf.etl.processors.participantcentric.ParticipantCentricProcessor
import io.kf.etl.processors.participantcommon.ParticipantCommonProcessor

object ETLMain extends App {
  val context = new DefaultContext()
  try {
    lazy val cliArgs: CLIParametersHolder = new CLIParametersHolder(args)
    import context._

    import scala.concurrent.ExecutionContext.Implicits._

    cliArgs.study_ids match {

      // Requires study_ids to run
      case Some(study_ids) =>
        println(s"Running Pipeline with study IDS {${study_ids.mkString(", ")}}")

        study_ids.foreach { studyId =>
          val dowloadData = DownloadProcessor(studyId)
          val participantCommon = ParticipantCommonProcessor(dowloadData)
          val fileCentric = FileCentricProcessor(dowloadData, participantCommon)
          val participantCentric = ParticipantCentricProcessor(dowloadData, participantCommon)
          IndexProcessor("file_centric", studyId, cliArgs.release_id.get, fileCentric)
          IndexProcessor("participant_centric", studyId, cliArgs.release_id.get, participantCentric)
          spark.sqlContext.clearCache()

        }

      // No Study IDs:
      case None =>
        println(s"No Study IDs provided - Nothing to run.")
        System.exit(-1)

    }


  } finally {
    context.close()
  }




}
