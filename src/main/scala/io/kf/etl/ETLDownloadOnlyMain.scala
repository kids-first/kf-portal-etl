package io.kf.etl

import io.kf.etl.context.DefaultContext
import io.kf.etl.processors.download.DownloadProcessor

object ETLDownloadOnlyMain extends App {

  DefaultContext.withContext(withES = false){ context =>
    import context.implicits._

    import scala.concurrent.ExecutionContext.Implicits._

    val study_ids = args.headOption.map(_.split(","))
    study_ids match {

      // Requires study_ids to run
      case Some(study_ids) =>
        println(s"Running Pipeline with study IDS {${study_ids.mkString(", ")}}")

        study_ids.foreach { studyId =>
          println(s"Running StudyId $studyId")
          DownloadProcessor(studyId, sink = true)
          spark.sqlContext.clearCache()
        }

      // No Study IDs:
      case None =>
        throw new IllegalArgumentException("No Study IDs provided - Nothing to run.")

    }


  }


}
